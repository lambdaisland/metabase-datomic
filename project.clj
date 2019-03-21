(defproject metabase/datomic-driver "1.0.0-SNAPSHOT-0.9.5697"
  :min-lein-version "2.5.0"

  :dependencies
  [#_[com.datomic/datomic-pro "0.9.5561.62" :exclusions [org.slf4j/slf4j-nop]]
   [org.clojure/clojure "1.10.0"]
   [com.datomic/datomic-free "0.9.5697"
    :exclusions [org.slf4j/jcl-over-slf4j
                 org.slf4j/jul-to-slf4j
                 org.slf4j/log4j-over-slf4j
                 org.slf4j/slf4j-nop]]]

  :profiles
  {:provided
   {:dependencies [[metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "datomic.metabase-driver.jar"}})
