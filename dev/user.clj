(ns user)

(defmacro jit
  "Just in time loading of dependencies."
  [sym]
  `(do
     (require '~(symbol (namespace sym)))
     (find-var '~sym)))

(defn setup-driver! []
  ((jit metabase.plugins.initialize/init-plugin-with-info!)
   ((jit yaml.core/from-file)
    ((jit clojure.java.io/file) "../metabase-datomic/resources/metabase-plugin.yaml"))))

(defn start-metabase! []
  (let [start-web-server! (jit metabase.server/start-web-server!)
        app               (jit metabase.handler/app)
        init!             (jit metabase.core/init!)]
    (start-web-server! app)
    (init!)
    (setup-driver!)))

(defn open-metabase []
  ((jit clojure.java.browse/browse-url) "http://localhost:3000"))


(comment
  (start-metabase!)
  (open-metabase)

  ((jit user.setup/setup-all))
  )
