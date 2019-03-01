(merge-env!
  :resource-paths #{"src"}
  :dependencies
  (template [[org.clojure/clojure       "1.9.0"]
             [io.aviso/logging          "0.3.1"]
             [ring                      "1.7.1"]
             [cheshire                  "5.8.1"]
             [amazonica                 "0.3.133"]
             [com.climate/claypoole     "1.1.4"]
             [org.clojure/java.jdbc     "0.7.7"]
             [org.postgresql/postgresql "42.2.4"]
             [hikari-cp                 "2.6.0"]
             [clj-statsd                "0.4.0"]
             [adzerk/env                "0.4.0"]]))

(task-options!
 jar  {:main 'adzerk.kinesis-adapter.main}
 sift {:include #{#"\.jar$"}}
 aot  {:namespace #{'adzerk.kinesis-adapter.main}})

(deftask build
  "Builds a standalone jar."
  []
  (comp (aot) (uber) (jar) (sift) (target)))
