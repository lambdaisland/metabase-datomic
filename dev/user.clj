(ns user)

(defmacro jit
  "Just in time loading of dependencies."
  [sym]
  `(do
     (require '~(symbol (namespace sym)))
     (find-var '~sym)))

(defn setup-driver! []
  (-> "../metabase-datomic/resources/metabase-plugin.yaml"
      ((jit clojure.java.io/file))
      ((jit yaml.core/from-file))
      ((jit metabase.plugins.initialize/init-plugin-with-info!))))

(defn open-metabase []
  ((jit clojure.java.browse/browse-url) "http://localhost:3000"))

(defn go []
  (let [start-web-server! (jit metabase.server/start-web-server!)
        app               (jit metabase.handler/app)
        init!             (jit metabase.core/init!)]
    (start-web-server! app)
    (init!)
    (setup-driver!)
    (open-metabase)))

(defn setup! []
  ((jit user.setup/setup-all)))

(defn refresh []
  ((jit clojure.tools.namespace.repl/set-refresh-dirs)
   "../metabase-datomic/src"
   "../metabase-datomic/dev"
   "../metabase-datomic/test")
  ((jit clojure.tools.namespace.repl/refresh)))

(defn refer-repl []
  (require '[user.repl :refer :all]
           '[user :refer :all]
           '[clojure.repl :refer :all]
           '[sc.api :refer :all]))
