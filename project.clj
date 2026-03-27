(defproject connector "0.1.0-SNAPSHOT"
  :main connector.core
  :description "data connector service"
  :aot [connector.core]
  :min-lein-version "2.0.0"

  :jvm-opts ["-server" "-Xmx2g"
             "-Duser.language=en"
             "--add-opens=java.base/java.io=ALL-UNNAMED"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
             "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"]

  :dependencies [[org.clojure/clojure "1.12.0"]
                 [mount "0.1.23"]
                 [com.grammarly/omniconf "0.5.2"]
                 [compojure "1.6.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.5.1"]
                 [ring/ring-jetty-adapter "1.9.6"]
                 [com.taoensso/timbre "6.8.0"]
                 [yieldbot/serializable-fn "0.1.3"]
                 [yieldbot/flambo "0.9.0-SNAPSHOT"]
                 ;; CSV parsing for single-pass clean
                 [org.clojure/data.csv "1.1.0"]
                 [com.fasterxml.jackson.core/jackson-core "2.15.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.15.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.15.2"]
                 [com.fasterxml.jackson.module/jackson-module-scala_2.12 "2.15.2"]]

  :plugins [[lein-ring "0.12.5"]
            [lein-cljfmt "0.9.2"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "1.4.3"]]

  :profiles
  {:dev {:dependencies
         [[javax.servlet/servlet-api "2.5"]
          [ring/ring-mock "0.3.2"]]}
   :uberjar {:aot :all}
   :provided
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.5.1"]
     [org.apache.spark/spark-sql_2.12 "3.5.1"]
     [org.apache.spark/spark-hive_2.12 "3.5.1"]]}})