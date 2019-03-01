(ns adzerk.kinesis-adapter.main
  (:gen-class)
  (:require [clojure.tools.logging :as log]))

(defn -main [& args]
  (require 'adzerk.kinesis-adapter.worker)
  (let [start! @(resolve 'adzerk.kinesis-adapter.worker/start!)
        stop!  @(resolve 'adzerk.kinesis-adapter.worker/stop!)]
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread. #(do (stop!) (shutdown-agents))))
    (start!)))
