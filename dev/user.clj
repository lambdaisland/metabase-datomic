(ns user
  (:require [clojure.java.io :as io]))

(def paths ["src" "dev" "test" "resources"])

(defmacro jit
  "Just in time loading of dependencies."
  [sym]
  `(do
     (require '~(symbol (namespace sym)))
     (find-var '~sym)))

(defn add-classpaths []
  (doseq [cp paths]
    ((jit metabase.plugins.classloader/add-url-to-classpath!)
     (.toURL
      (.toURI
       (io/file (str "../metabase-datomic/" cp)))))))

(defn start-metabase! []
  (let [start-web-server! (jit metabase.server/start-web-server!)
        app               (jit metabase.handler/app)
        init!             (jit metabase.core/init!)]
    (start-web-server! app)
    (init!)))

(defn open-metabase []
  ((jit clojure.java.browse/browse-url) "http://localhost:3000"))

(comment
  (add-classpaths)

  (require 'metabase.driver.datomic)
  (start-metabase!)
  (open-metabase)

  ((jit user.setup/setup-all))

  )
