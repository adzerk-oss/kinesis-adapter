(ns adzerk.kinesis-adapter.util
  (:require
    [clj-statsd :as s]
    [clojure.string :as string]
    [clojure.java.shell :as shell])
  (:import
    [java.io File]
    [java.nio.file Files StandardCopyOption FileVisitOption]
    [java.lang.management ManagementFactory]))

(defn name*
  [x]
  (if (instance? clojure.lang.Named x) (name x) (str x)))

(defmacro with-let
  [[bind & more] & body]
  `(let [~bind ~@more] ~@body ~bind))

(defmacro guard
  [& body]
  `(try ~@body (catch Throwable t#)))

(defn thread-uuid
  []
  (-> "%s|%s|%s" (format (-> (shell/sh "hostname") :out (or "") string/trim)
                         (.getName (ManagementFactory/getRuntimeMXBean))
                         (.getId (Thread/currentThread)))))

(defn timed
  [f k]
  (fn [& args]
    (let [start (System/currentTimeMillis)]
      (try (apply f args)
           (finally (let [end (System/currentTimeMillis)]
                      (s/timing k (- end start))))))))
