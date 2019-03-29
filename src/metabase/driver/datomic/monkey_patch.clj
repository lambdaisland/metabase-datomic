(ns metabase.driver.datomic.monkey-patch
  (:require [toucan.models]
            [toucan.hydrate]))

;; Prevent Toucan from doing a keyword-call on every var in the systems, as this
;; causes some of Datomic's internal cache structures to throw an exception.
;; https://github.com/metabase/toucan/issues/55

;; We can drop this as soon as Metabase has upgraded to Toucan 1.12.0

(defn- require-model-namespaces-and-find-hydration-fns []
  (reduce
   (fn [coll ns]
     (reduce
      (fn [coll sym-var]
        (let [model (var-get (val sym-var))]
          (if (and (record? model) (toucan.models/model? model))
            (reduce #(assoc %1 %2 model)
                    coll
                    (toucan.models/hydration-keys model))
            coll)))
      coll
      (ns-publics ns)))
   {}
   (all-ns)))

(alter-var-root #'toucan.hydrate/require-model-namespaces-and-find-hydration-fns
                (constantly require-model-namespaces-and-find-hydration-fns))
