(ns adzerk.kinesis-adapter.worker
  (:import
    [java.io File]
    [org.eclipse.jetty.server.handler StatisticsHandler])
  (:require
    [com.climate.claypoole :as cp]
    [cheshire.core :as json]
    [clj-statsd :as s]
    [clojure.tools.logging :as log]
    [adzerk.kinesis-adapter.util :as util]
    [adzerk.kinesis-adapter.api :as api]
    [adzerk.kinesis-adapter.config :as config]
    [ring.adapter.jetty :refer [run-jetty]]
    [ring.middleware.params :refer [wrap-params]]
    [ring.middleware.stacktrace :refer [wrap-stacktrace]]))

;; stats ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/setup "127.0.0.1" 8125)

(declare stop!)

(defonce webserver  (atom nil))
(defonce worker     (atom false))
(defonce workers    (atom []))
(defonce paused?    (atom false))

;; helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ingest-paused?
  []
  (or @paused? (.exists (File. config/down-for-maintenance-file))))

(defn wrap-route
  [next-handler method uri-path handler]
  (fn [{:keys [uri request-method] :as req}]
    (let [route-matches? (= [request-method uri] [method uri-path])]
      ((if route-matches? handler next-handler) req))))

(defn not-found-handler
  [req]
  {:status 404 :body "NOT FOUND"})

(defn ok-handler
  [req]
  {:status 200 :body "OK"})

(defn download-handler
  [req]
  (let [{:strs [table s3path]} (json/parse-string (slurp (:body req)))]
    (cond (ingest-paused?) {:status 500 :body "SERVER UNAVAILABLE"}
          (api/s3-download-and-ingest! table s3path) {:status 200 :body "OK"}
          :else {:status 500 :body "SERVER ERROR"})))

(defn shutdown-handler
  [_]
  (stop!)
  {:status 200 :body "STOPPED"})

(defn status-handler
  [_]
  {:status 200 :body (json/generate-string
                       {:maintenance_mode (ingest-paused?)
                        :files_ingesting  (api/files-ingesting?)
                        :records_in_queue (api/records-in-queue?)})})

(def ring-app
  (-> not-found-handler
      (wrap-route :get  "/_health"   ok-handler)
      (wrap-route :get  "/status"    status-handler)
      (wrap-route :post "/ingest"    download-handler)
      (wrap-route :post "/shutdown"  shutdown-handler)
      (wrap-params)
      (wrap-stacktrace)))

(let [filetypes (atom (cycle config/valid-file-types))]
  (defn next-filetype!
    []
    (first (swap! filetypes next))))

(defn jetty-configurator
  "Graceful shutdown of jetty server."
  [server]
  (doto server (.setStopTimeout 60000) (.setStopAtShutdown true)))

(defn do-work
  [i]
  (log/info "worker: starting" i)
  (while @worker
    (when-not
      (try (api/get-records-and-put! (next-filetype!))
           (catch Throwable t
             (log/error t "worker: uncaught exception")))
      (Thread/sleep 100)))
  (log/info "worker: stopped" i))

;; API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-web!
  [& _]
  (locking webserver
    (reset! paused? false)
    (let [opts {:join? false
                :configurator jetty-configurator
                :port (read-string config/KA_WEBSERVER_PORT)}]
      (swap! webserver #(when-not % (run-jetty #'ring-app opts))))))

(defn stop-web!
  [& _]
  (locking webserver
    (reset! paused? true)
    (while (< 0 @api/ingesters) (Thread/sleep 100))
    (swap! webserver #(some-> % .stop))))

(defn start-worker!
  [& _]
  (locking worker
    (when (compare-and-set! worker false true)
      (s/increment :ka.start)
      (dotimes [i (read-string config/KA_MAX_WORKER_CONCURRENCY)]
        (swap! workers conj (future (do-work i)))))))

(defn stop-worker!
  [& _]
  (locking worker
    (when (compare-and-set! worker true false)
      (swap! workers #(doseq [w %] @w)))))

(defn start!
  [& _]
  (start-web!)
  (start-worker!))

(defn stop!
  [& _]
  (reset! paused? true)
  (stop-worker!)
  (stop-web!))
