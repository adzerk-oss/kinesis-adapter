(ns adzerk.kinesis-adapter.config
  (:require
    [adzerk.env :as env]
    [clojure.java.io :as io]))

(env/def
  KA_IMPRESSIONS_STREAM       :required
  KA_CLICKS_STREAM            :required
  KA_CONVERSIONS_STREAM       :required
  KA_CUSTOMEVENTS_STREAM      :required
  KA_SWITCHOVER_TIME          "2019_01_17_1800"
  KA_WEBSERVER_PORT           "3000"
  KA_MAX_RECORD_BACKLOG       "250000"
  KA_MAX_INGEST_CONCURRENCY   "8"
  KA_MAX_WORKER_CONCURRENCY   "32")

(def pool-size
  (+ (read-string KA_MAX_INGEST_CONCURRENCY)
     (read-string KA_MAX_WORKER_CONCURRENCY)
     4))

(def db
  {:host              "localhost:5432"
   :dbname            "postgres"
   :user              "postgres"
   :pass              ""
   :minimum-idle      pool-size
   :maximum-pool-size pool-size})

(def file-type->stream
  {"impressions"  KA_IMPRESSIONS_STREAM
   "clicks"       KA_CLICKS_STREAM
   "conversions"  KA_CONVERSIONS_STREAM
   "customEvents" KA_CUSTOMEVENTS_STREAM})

(def valid-file-types
  (set (keys file-type->stream)))

(defn parse-filename
  "Given a log file name returns the file type as in the kinesis stream mapping
  from file type to stream (config.clj)."
  [filename]
  ;;  7_impressions_2019_01_01_1430_7966_engine-i-089f81ef9893147c8.log
  ;;       5_clicks_2019_01_01_0000_23285_engine-i-005abfe5f969d835b.log
  ;;  5_conversions_2019_01_01_0000_7785_engine-i-0f96396da7de1df2f.log
  ;; 5_customEvents_2019_01_01_0000_23285_engine-i-005abfe5f969d835b.log
  (let [[_ _ type time]
        (re-find
          #"^([0-9]+_)?([^_]+)_([0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{4})_.*\.log"
          (.getName (io/file filename)))]
    {:type type :time time}))

(defn file-name->type
  "Given a log file name returns the file type as in the kinesis stream mapping
   from file type to stream (config.clj)."
  [filename]
  (-> filename parse-filename :type valid-file-types))

(def down-for-maintenance-file
  "/down-for-maintenance")
