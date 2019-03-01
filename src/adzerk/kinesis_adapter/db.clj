(ns adzerk.kinesis-adapter.db
  (:require
    [clj-statsd :as s]
    [clojure.set :as set]
    [clojure.java.io :as io]
    [hikari-cp.core :as hikari]
    [clojure.java.jdbc :as jdbc]
    [clojure.tools.logging :as log]
    [clojure.string :as string :refer [join split]]
    [adzerk.kinesis-adapter.config :as config])
  (:import
    [java.sql SQLException]
    [org.postgresql.util PGobject]))

(declare ^:dynamic *tx* QUERY EXECUTE!)

(def this-ns (str *ns*))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; strint.clj -- String interpolation for Clojure
;; originally proposed/published at http://muckandbrass.com/web/x/AgBP

;; by Chas Emerick <cemerick@snowtide.com>
;; December 4, 2009

;; Copyright (c) Chas Emerick, 2009. All rights reserved.  The use
;; and distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(defn- silent-read
  "Attempts to clojure.core/read a single form from the provided String, returning
   a vector containing the read form and a String containing the unread remainder
   of the provided String.  Returns nil if no valid form can be read from the
   head of the String."
  [s]
  (try
    (let [r (-> s java.io.StringReader. java.io.PushbackReader.)]
      [(read r) (slurp r)])
    (catch Exception e))) ; this indicates an invalid form -- the head of s is just string data

(defn interpolate
  "Yields a seq of Strings and read forms."
  ([s atom?]
    (lazy-seq
      (if-let [[form rest] (silent-read (subs s (if atom? 2 1)))]
        (cons form (interpolate (if atom? (subs rest 1) rest)))
        (cons (subs s 0 2) (interpolate (subs s 2))))))
  ([^String s]
    (if-let [start (->> ["~{" "~("]
                     (map #(.indexOf s %))
                     (remove #(== -1 %))
                     sort
                     first)]
      (lazy-seq (cons
                  (subs s 0 start)
                  (interpolate (subs s start) (= \{ (.charAt s (inc start))))))
      [s])))

(defmacro <<
  "Takes a single string argument and emits a str invocation that concatenates
   the string data and evaluated expressions contained within that argument.
   Evaluation is controlled using ~{} and ~() forms.  The former is used for
   simple value replacement using clojure.core/str; the latter can be used to
   embed the results of arbitrary function invocation into the produced string."
  [string]
  `(str ~@(interpolate string)))

;; postgres data types ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol jdbc/IResultSetReadColumn
  PGobject ;; eg. jsonb
  (result-set-read-column [val _ _] (.getValue val)))

(defmulti clj->pg
  "Wraps value in the postgres parameter type corresponding to pgtype."
  (fn [pgtype value] pgtype))

(defmethod clj->pg :default
  [_ value]
  value)

(defmethod clj->pg ::_int4
  [_ value]
  (-> (jdbc/get-connection *tx*)
      (.createArrayOf "integer" (into-array value))))

(defmethod clj->pg ::jsonb
  [pgtype value]
  (doto (PGobject.) (.setType (name pgtype)) (.setValue value)))

(defn row->pg
  "Given a map of column types (see table-column-types) and a row map, returns
  a row map with the values coerced to postgres parameters via clj->pg accord-
  ing to the types of the associated columns in the table schema."
  [types row]
  (->> (keys row)
       (map #(vector % (clj->pg (types %) (row %))))
       (into {})))

;; helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn assert-tx
  []
  (assert (bound? #'*tx*) "operation must be performed within a transaction"))

(defn jdbc-url
  "Forms a JDBC postgres connection URL."
  [host port dbname]
  (format "jdbc:postgresql://%s:%s/%s" host port dbname))

(defonce datasource
  (delay
    (let [{:keys [host port dbname user pass] :as cfg} config/db]
      (hikari/make-datasource
        (merge {:minimum-idle        20
                :maximum-pool-size   20
                :adapter             "postgresql"
                :jdbc-url            (jdbc-url host port dbname)
                :username            user
                :password            pass}
               (dissoc cfg :host :port :dbname :user :pass))))))

(defmacro with-retry
  "Like try, allowing recur from catch."
  [[bind val] & body]
  `(let [~bind ~val]
     (loop [x# ~bind]
       (if (not= x# ~bind) x# (recur (try ~@body))))))

(defmacro with-tx
  "Begins a database transaction and evaluates the body expressions within the
  transaction. Then if no exception was thrown the transaction is committed.
  See also clojure.java.jdbc/with-db-transaction. Isolation level is a keyword
  like :read-committed (dashes become spaces). Serialization failure and dead-
  lock detected errors are automatically retried."
  [isolation-level & body]
  `(with-retry [again# (atom nil)]
     (jdbc/with-db-transaction
       [x# {:datasource @datasource} {:isolation ~isolation-level}]
       (binding [*tx* x#] ~@body))
     (catch SQLException t#
       (comment
         ;; See: https://www.postgresql.org/docs/9.6/errcodes-appendix.html
         (if (#{"40001" "40P01"} (.getSQLState t#))
           again#
           (throw t#)))
       ;; We do not want to retry because we don't use the serializable iso-
       ;; lation level, and retries could result in writing duplicate records
       ;; to kinesis.
       (throw t#))))

(def table-column-types
  (memoize
    (fn [table]
      (jdbc/with-db-metadata [m {:datasource @datasource}]
        (->> (.getColumns m nil nil (name table) nil)
             (jdbc/metadata-query)
             (map (juxt (comp keyword :column_name)
                        (comp (partial keyword this-ns) :type_name)))
             (into {}))))))

(def table-pk-columns
  (memoize
    (fn [table]
      (jdbc/with-db-metadata [m {:datasource @datasource}]
        (->> (.getPrimaryKeys m nil nil (name table))
             (jdbc/metadata-query)
             (map :column_name))))))

(defn analyze-row
  "Computes data useful when compiling queries."
  [table row]
  (let [col-types (table-column-types table)
        pk-cols   (table-pk-columns table)
        coerced   (row->pg col-types row)
        columns   (map name (keys coerced))]
    {:table   (name table)
     :values  (vals coerced)
     :columns (map keyword columns)
     :cols    (join ", " columns)
     :?s      (join ", " (repeat (count columns) "?"))
     :pk-cols (join ", " pk-cols)
     :sets    (join ", " (->> (set/difference (set columns) (set pk-cols))
                              (map (fn [x] (<< "~{x} = excluded.~{x}")))))}))

;; API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn QUERY         [& xs] (assert-tx) (apply jdbc/query *tx* xs))
(defn EXECUTE!      [& xs] (assert-tx) (apply jdbc/execute! *tx* xs))
(defn INSERT!       [& xs] (assert-tx) (apply jdbc/insert-multi! *tx* xs))
(defn INSERT-MULTI! [& xs] (assert-tx) (apply jdbc/insert-multi! *tx* xs))
