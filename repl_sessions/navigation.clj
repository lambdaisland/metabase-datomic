(ns navigation
  (:require [datomic.api :as d]
            [clojure.string :as str]))

{:relationships {:journal-entry
                 {:account
                  {:path [:journal-entry/journal-entry-lines :journal-entry-line/account]
                   :target :account}}}

 :tx-filter
 (fn [db ^datomic.Datom datom]
   (let [tx-tenant (get-in (datomic.api/entity db (.tx datom)) [:tx/tenant :db/id])]
     (or (nil? tx-tenant) (= 17592186046521 tx-tenant))))}



{:rules
 [[(sub-accounts ?p ?c)
   [?p :account/children ?c]]
  [(sub-accounts ?p ?d)
   [?p :account/children ?c]
   (sub-accounts ?c ?d)]
  ]

 :relationships
 {:account
  {:journal-entry-lines
   {:path [:journal-entry-line/_account]
    :target :journal-entry-line}

   :subaccounts
   {:path [sub-accounts]
    :target :account}

   :parent-accounts
   {:path [_sub-accounts]
    :target :account}
   }

  :journal-entry-line
  {:fiscal-year
   {:path [:journal-entry/_journal-entry-lines
           :ledger/_journal-entries
           :fiscal-year/_ledgers]
    :target :fiscal-year}}

  }}



(path-binding '?jel '?fj '[:journal-entry/_journal-entry-lines
                           :ledger/_journal-entries
                           :fiscal-year/_ledgers])

(d/q '{:find [?e]
       :where [[?e :db/ident]]} (db))


(->> (for [path (->> #_[:journal-entry/_journal-entry-lines
                        :ledger/_journal-entries
                        :fiscal-year/_ledgers
                        :company/_fiscal-year]
                     [:fiscal-year/_accounts
                      :company/_fiscal-year]
                     reverse
                     (reductions conj [])
                     (map reverse)
                     next)
           :let [[rel _] path
                 table (keyword (subs (name rel) 1))]]
       [table :company {:path (vec path)
                        :target :company}])
     (reduce (fn [c p]
               (assoc-in c (butlast p) (last p))) {}))


{:fiscal-year
 {:company {:path [:company/_fiscal-year]
            :target :company}}
 :ledger
 {:company {:path [:fiscal-year/_ledgers
                   :company/_fiscal-year]
            :target :company}}
 :journal-entry
 {:company {:path [:ledger/_journal-entries
                   :fiscal-year/_ledgers
                   :company/_fiscal-year]
            :target :company}}
 :journal-entry-line
 {:company {:path [:journal-entry/_journal-entry-lines
                   :ledger/_journal-entries
                   :fiscal-year/_ledgers
                   :company/_fiscal-year]
            :target :company}}
 :account
 {:company {:path [:fiscal-year/_accounts
                   :company/_fiscal-year]
            :target :company}}}

(user/refer-repl)

(vec (sort (d/q '{:find [?ident]
                  :where [[?e :db/ident ?ident]
                          [?e :db/valueType :db.type/ref]
                          [?e :db/cardinality :db.cardinality/many]]}
                (db eeleven-url))))


(def hierarchy
  {:tenant/companies
   {:company/fiscal-years
    {:fiscal-year/account-matches
     {}

     :fiscal-year/accounts
     {:account/children
      {}

      :account/contact-cards
      {}

      ;; :account-match/journal-entry-lines
      ;; {}
      }

     :fiscal-year/currency-options
     {}

     :fiscal-year/fiscal-periods
     {}

     :fiscal-year/ledgers
     {:ledger/journal-entries
      {:journal-entry/journal-entry-lines
       {}

       }

      :ledger/tax-entries
      {:tax-entry/tax-lines
       {}}}

     :fiscal-year/tax-accounts
     {}

     :fiscal-year/analytical-year
     {:analytical-year/entries
      {:analytical-entry/analytical-entry-lines
       {}}

      :analytical-year/dimensions
      {}

      :analytical-year/tracked-accounts
      {}}
     }

    :company/bank-reconciliations
    {:bank-reconciliation/bank-statement-lines
     {}

     ;; :bank-reconciliation/journal-entry-lines
     ;; {}
     }

    :company/bank-statements
    {:bank-statement/bank-statement-lines {}}

    :company/contact-cards
    {}

    :company/documents
    {}

    :company/payments
    {:payment/payables
     {}

     :payment/receivables
     {}}

    :company/receipts
    {:receipt/payables
     {}

     :receipt/receivables
     {}}

    :company/sequences
    {}}

   :tenant/org-units
   {:org-unit/children
    {}}

   :tenant/users
   {:user/roles
    {:role/roles
     {}}}})

(defn maps->paths [m]
  (mapcat (fn [k]
            (cons [k]
                  (map (partial cons k) (maps->paths (get m k)))))
          (keys m)))

(defn reverse-path [p]
  (->> p
       reverse
       (mapv #(keyword (namespace %) (str "_" (name %))))))

(defn singularize [kw]
  ({:entries :entry
    :tax-entries :tax-entry
    :journal-entries :journal-entry
    :account-matches :account-match
    :analytical-year :analytical-year}
   kw
   (keyword (subs (name kw) 0 (dec (count (name kw)))))))

{:relationships
 (->
  (reduce
   (fn [rels path]
     (assoc-in rels [(singularize
                      (keyword (name (last path))))
                     :company] {:path (reverse-path path)
                                :target :company}))
   {}
   (maps->paths (:tenant/companies hierarchy)))
  (dissoc :childre :tracked-account :receivable :tax-line))}
