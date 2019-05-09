(ns query-checks
  (:require [datomic.api :as d]))

(user/refer-repl)


(d/q
 '{:where
   [(or
     [?journal-entry-line :journal-entry-line/account]
     [?journal-entry-line :journal-entry-line/amount]
     [?journal-entry-line :journal-entry-line/base-amount]
     [?journal-entry-line :journal-entry-line/description]
     [?journal-entry-line :journal-entry-line/due-date]
     [?journal-entry-line :journal-entry-line/flow]
     [?journal-entry-line :journal-entry-line/id]
     [?journal-entry-line :journal-entry-line/index])
    [?journal-entry-line :journal-entry-line/base-amount ?journal-entry-line|journal-entry-line|base-amount]
    [?journal-entry-line :journal-entry-line/account ?journal-entry-line|journal-entry-line|account->account]
    [?journal-entry-line|journal-entry-line|account->account :account/currency ?journal-entry-line|journal-entry-line|account->account|account|currency]
    [?journal-entry-line :journal-entry-line/flow ?journal-entry-line|journal-entry-line|flow->elvn]],
   :find
   [?journal-entry-line ?journal-entry-line|journal-entry-line|base-amount ?journal-entry-line|journal-entry-line|account->account|account|currency ?journal-entry-line|journal-entry-line|flow->elvn]}
 (db eeleven-url))

(d/q
 '{:where
   [(or
     [?journal-entry-line :journal-entry-line/account]
     [?journal-entry-line :journal-entry-line/amount]
     [?journal-entry-line :journal-entry-line/base-amount]
     [?journal-entry-line :journal-entry-line/description]
     [?journal-entry-line :journal-entry-line/due-date]
     [?journal-entry-line :journal-entry-line/flow]
     [?journal-entry-line :journal-entry-line/id]
     [?journal-entry-line :journal-entry-line/index])
    [?journal-entry-line :journal-entry-line/base-amount ?journal-entry-line|journal-entry-line|base-amount]
    [?journal-entry-line :journal-entry-line/account ?journal-entry-line|journal-entry-line|account->account]
    [?journal-entry-line|journal-entry-line|account->account :account/currency ?journal-entry-line|journal-entry-line|account->account|account|currency]
    [?journal-entry-line :journal-entry-line/flow ?journal-entry-line|journal-entry-line|flow->elvn]
    [?journal-entry-line :journal-entry-line/base-amount ?journal-entry-line|journal-entry-line|base-amount]],
   :order-by
   [[:asc (:db/id ?journal-entry-line)]
    [:asc (:journal-entry-line/base-amount ?journal-entry-line)]
    [:asc (:account/currency ?journal-entry-line|journal-entry-line|account->account)]
    [:asc (:db/id ?journal-entry-line|journal-entry-line|flow->elvn)]],
   :find
   [?journal-entry-line
    ?journal-entry-line|journal-entry-line|base-amount
    ?journal-entry-line|journal-entry-line|account->account|account|currency
    ?journal-entry-line|journal-entry-line|flow->elvn
    (sum ?journal-entry-line|journal-entry-line|base-amount)]
   :select
   [(:db/id ?journal-entry-line)
    (:journal-entry-line/base-amount ?journal-entry-line)
    (:account/currency ?journal-entry-line|journal-entry-line|account->account)
    (:db/id
     ?journal-entry-line|journal-entry-line|flow->elvn)
    (sum ?journal-entry-line|journal-entry-line|base-amount)],
   #_#_:with (?journal-entry-line)}
 (db eeleven-url)
 )
