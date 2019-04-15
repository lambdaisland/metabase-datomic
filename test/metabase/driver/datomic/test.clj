(ns metabase.driver.datomic.test
  (:require  [clojure.test :as test]
             [metabase.driver :as driver]))

(defmacro with-datomic [& body]
  `(driver/with-driver :datomic
     ~@body))

(defmacro refer-macro [m]
  (let [name (symbol (name m))]
    `(do
       (defmacro ~name
         [& body#]
         (cons '~m body#))
       (alter-meta! (resolve '~name)
                    merge
                    (select-keys (meta (resolve '~m)) [:arglists :doc])))))

(refer-macro clojure.test/is)
(refer-macro clojure.test/are)
(refer-macro clojure.test/testing)
