(ns adzerk.kinesis-adapter.api
  (:require
    [clojure.java.io :as io]
    [clojure.string :as string]
    [clj-statsd :as s]
    [clojure.tools.logging :as log]
    [cheshire.core :as json]
    [amazonica.aws.s3 :as s3]
    [amazonica.aws.dynamodbv2 :as ddb]
    [amazonica.aws.kinesis :as kinesis]
    [adzerk.kinesis-adapter.config :as config]
    [adzerk.kinesis-adapter.util :as util :refer [timed guard name*]]
    [adzerk.kinesis-adapter.db :as db :refer [<< with-tx QUERY INSERT-MULTI! EXECUTE!]])
  (:import
    [java.sql SQLException]
    [java.util.zip GZIPInputStream]
    [com.amazonaws.services.s3.model AmazonS3Exception]
    [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))

;; setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare records-in-queue?)

(s/setup "127.0.0.1" 8125)

(def max-backlog
  (read-string config/KA_MAX_RECORD_BACKLOG))

(def max-ingesters
  (read-string config/KA_MAX_INGEST_CONCURRENCY))

(defn should-process?
  "Responsibility for writing records to kinesis is determined by the switch-
   over date and the timestamp in the filename. If the filename's timestamp is
   after the agreed-upon switchover date we handle it, otherwise it's the re-
   sponsibility of logsweeper on the engines."
  [filename]
  (some-> (config/parse-filename filename)
          :time
          (.compareTo config/KA_SWITCHOVER_TIME)
          pos?))

;; S3 helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn split-path
  "Converts the <bucket>/<key> format to vector of bucket, key."
  [s3path]
  (string/split s3path #"/" 2))

(defn input-stream
  "Given an s3 get result returns the content input stream, gunzipped."
  [{:keys [input-stream] {:keys [content-encoding]} :object-metadata}]
  (if (not= content-encoding "gzip") input-stream (GZIPInputStream. input-stream)))

(defn s3-reader
  "..."
  [s3path]
  (let [[bucket-name key-name] (split-path s3path)]
    (io/reader (input-stream (s3/get-object :bucket-name bucket-name :key key-name)))))

;; dynamo helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn dynamo-tx-complete?
  [table-name s3path]
  (= -1 (:ka_lease (:item (ddb/get-item :consistent-read true
                                        :table-name table-name
                                        :key {:s3path s3path})))))

(defn dynamo-tx-commit!
  [table-name s3path tx-uid tx-expire]
  (ddb/update-item
    :table-name table-name
    :key {:s3path s3path}
    :condition-expression "ka_claimed = :v1 AND ka_lease = :v2"
    :update-expression "SET ka_lease = :v3"
    :expression-attribute-values {":v1" tx-uid ":v2" tx-expire ":v3" -1}))

(defn dynamo-tx-begin!
  [table-name s3path & {:keys [ttl] :or {ttl (* 1000 60 5)}}]
  (let [now (System/currentTimeMillis)
        uid (util/thread-uuid)
        expire (+ now ttl)]
    (ddb/update-item
      :table-name table-name
      :key {:s3path s3path}
      :condition-expression "attribute_not_exists(ka_lease) OR ka_lease BETWEEN :v0 AND :v2"
      :update-expression "SET ka_claimed = :v1, ka_lease = :v3"
      :expression-attribute-values {":v0" 0 ":v1" uid ":v2" now ":v3" expire})
    #(dynamo-tx-commit! table-name s3path uid expire)))

;; kinesis helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str->buf
  [^String x]
  (java.nio.ByteBuffer/wrap (.getBytes x)))

(defn partition-key
  []
  (str (java.util.UUID/randomUUID)))

(defn make-record
  [{:keys [data]}]
  {:partition-key (str (java.util.UUID/randomUUID)) :data (str->buf data)})

(defn put-records!
  [stream-name records]
  (->> (mapv make-record records)
       (kinesis/put-records stream-name)
       :records
       (map merge records)))

;; postgres helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn check-backlog
  [n]
  (QUERY ["SELECT * FROM check_backlog(?::int)" n]))

(defn table-has-rows
  []
  (boolean (first (QUERY ["SELECT 1 FROM log_record LIMIT 1"]))))

(defn delete-records!
  [ids]
  (let [n  (count ids)
        ?s (string/join "," (repeat n "?"))]
    (EXECUTE! (into [(<< "DELETE FROM log_record WHERE id IN (~{?s})")] ids))))

(defn get-records
  [filetype max-records]
  (QUERY ["SELECT * FROM get_log_records(?::text, ?::int)"
          filetype max-records]))

(defn make-row
  [filetype]
  (partial hash-map :type filetype :data))

(defn ingest-records!
  [table filetype lines]
  (when-let [rows (some->> lines (seq) (map (make-row filetype)))]
    (INSERT-MULTI! table rows)))

;; ingest helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce ingesters (atom 0))

(set-validator!
  ingesters
  #(or (< % max-ingesters)
       (throw (ex-info "throttled" {:throttled true}))))

;; misc. ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- not-found?
  [ex]
  (= "NoSuchKey" (.getErrorCode ex)))

(defn- backlog?
  [ex]
  (= "BCKLG" (.getSQLState ex)))

(defn- s3-download-and-ingest!*
  [table s3path]
  (let [filetype        (config/file-name->type s3path)
        ingest-records! (timed ingest-records! :ka.ingestrecords.time)]
    (cond (not (should-process? s3path)) [:switchover]
          (not (and table s3path filetype)) [:malformed]
          (not (guard (swap! ingesters inc))) [:throttled true]
          :else (try (with-tx :read-committed (check-backlog max-backlog))
                     (if (dynamo-tx-complete? table s3path)
                       [:txcomplete]
                       (let [dynamo-tx-commit! (dynamo-tx-begin! table s3path)]
                         (with-open [rdr (s3-reader s3path)]
                           (with-tx :read-committed
                             (->> (line-seq rdr)
                                  (ingest-records! "log_record" filetype)
                                  (count)
                                  (s/gauge :ka.s3file.size))
                             (dynamo-tx-commit!)
                             [:success]))))
                     (catch ConditionalCheckFailedException ex1 [:txfailed true])
                     (catch AmazonS3Exception ex2 (if (not-found? ex2) [:notfound] [:s3error ex2]))
                     (catch SQLException ex0 (if (backlog? ex0) [:throttled true] [:sqlerror ex0]))
                     (catch Throwable ex3 [:error ex3])
                     (finally (swap! ingesters dec))))))

(defn tagvec
  [tagmap]
  (reduce-kv #(conj %1 (str (name* %2) ":" (name* %3))) [] tagmap))

;; API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn records-in-queue?
  []
  (with-tx :read-committed (table-has-rows)))

(defn files-ingesting?
  []
  (< 0 @ingesters))

(defn s3-download-and-ingest!
  [table s3path]
  (let [tags (atom {:ka.table table :ka.status :impossible})]
    (try (let [[status retry?] (s3-download-and-ingest!* table s3path)
               ex (when (instance? Throwable retry?) retry?)]
           (swap! tags assoc :ka.status status)
           (cond ex (log/error ex status table s3path)
                 (#{:success} status) (log/info status table s3path)
                 (not (#{:throttled} status)) (log/warn status table s3path))
           (not retry?))
         (finally (s/increment :ka.ingest 1 1 (tagvec @tags))))))

(defn get-records-and-put!
  [filetype]
  (try (let [stream-name  (config/file-type->stream filetype)
             records      (atom nil)
             records-ok   (atom nil)
             put-records! (timed put-records! :ka.putrecords.time)]
         (with-tx :read-committed
           (some->> (seq (get-records filetype 500))
                    (put-records! stream-name)
                    (reset! records)
                    (remove :error-code)
                    (reset! records-ok)
                    (map :id)
                    (seq)
                    (delete-records!)))
         (when (seq @records)
           (let [n-rec (count @records)
                 n-ok  (count @records-ok)
                 n-err (- n-rec n-ok)]
             (s/gauge :ka.batch.size n-rec)
             (s/increment :ka.batch 1 1 (tagvec {:ka.status :success}))
             (s/increment :ka.record n-ok 1 (tagvec {:ka.status :success}))
             (s/increment :ka.record n-err 1 (tagvec {:ka.status :error}))))
         @records)
       (catch Throwable ex
         (log/error ex :error filetype)
         (s/increment :ka.batch 1 1 (tagvec {:ka.status :error})))))
