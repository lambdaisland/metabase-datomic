(ns query-checks
  (:require [datomic.api :as d]
            [clojure.set :as set]))



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
 (db eeleven-url))

(d/q
 '{:where
   [(or
     [?fiscal-year :fiscal-year/account-matches]
     [?fiscal-year :fiscal-year/accounts]
     [?fiscal-year :fiscal-year/analytical-year]
     [?fiscal-year :fiscal-year/currency-options]
     [?fiscal-year :fiscal-year/end-date]
     [?fiscal-year :fiscal-year/fiscal-periods]
     [?fiscal-year :fiscal-year/id]
     [?fiscal-year :fiscal-year/ledgers]
     [?fiscal-year :fiscal-year/start-date]
     [?fiscal-year :fiscal-year/tax-accounts])
    [?fiscal-year :fiscal-year/ledgers 17592186045807]],
   :find [(count ?fiscal-year)],
   :select [(count ?fiscal-year)],
   :with ()}
 (db eeleven-url))

(d/q
 '{:find [?ledger ?ledger|ledger|journal-entries->journal-entry]
   :where
   [[?ledger :ledger/journal-entries ?ledger|ledger|journal-entries->journal-entry]
    #_(or-join [?ledger ?ledger|ledger|journal-entries]
               [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
               (and [?ledger ]
                    [(ground ::nil) ?ledger|ledger|journal-entries]))
    [(= ?ledger|ledger|journal-entries->journal-entry 17592186046126)]]}
 (db eeleven-url))


(d/q
 '{:where
   [(or [?ledger :ledger/id]
        [?ledger :ledger/journal-entries]
        [?ledger :ledger/name]
        [?ledger :ledger/tax-entries])
    (or-join [?ledger ?ledger|ledger|journal-entries]
             [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
             (and [?ledger]
                  [(ground :metabase.driver.datomic.query-processor/nil) ?ledger|ledger|journal-entries]))
    (or-join
     [?ledger ?ledger|ledger|journal-entries->journal-entry]
     [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries->journal-entry]
     (and [?ledger]
          [(ground :metabase.driver.datomic.query-processor/nil) ?ledger|ledger|journal-entries->journal-entry]))
    (or-join
     [?ledger|ledger|journal-entries->journal-entry ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]
     [?ledger|ledger|journal-entries->journal-entry :journal-entry/id ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]
     (and [?ledger]
          [(ground :metabase.driver.datomic.query-processor/nil) ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]))
    [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
    [(println ?ledger|ledger|journal-entries 17592186046126)]],
   :find [?ledger|ledger|journal-entries ?ledger|ledger|journal-entries->journal-entry|journal-entry|id],
   :select [?ledger|ledger|journal-entries ?ledger|ledger|journal-entries->journal-entry|journal-entry|id],
   :with ()}

 (db eeleven-url))

(d/q
 '{:find [?ledger]
   :where
   [[?ledger :ledger/id]
    [?ledger :ledger/journal-entries ?je]
    #_[(prn ?je)]
    [(ground 17592186046126) ?je]
    ]
   }

 (db eeleven-url))
(set/intersection
 (set [[17592186045737] [17592186045674]])
 (set [[17592186046533] [17592186045737] [17592186045674] [17592186046540]]))


{:fields [[:field-id 1291] [:fk-> [:field-id 1291] [:field-id 1272]]],
 :filter [:=
          [:field-id 1291]
          [:value
           17592186046126
           {:base_type :type/FK,
            :special_type :type/FK,
            :database_type "db.type/ref"}]],
 :source-table 184,
 :limit 1048576,
 :join-tables
 [{:join-alias "journal-entry__via__journal-en",
   :table-id 191,
   :fk-field-id 1291,
   :pk-field-id 1275}]}


(d/q
 '{
   :find [?ledger|ledger|journal-entries
          ?ledger|ledger|journal-entries->journal-entry|journal-entry|id],

   :where [(or [?ledger :ledger/id]
               [?ledger :ledger/journal-entries]
               [?ledger :ledger/name]
               [?ledger :ledger/tax-entries])

           [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
           [?ledger|ledger|journal-entries :journal-entry/id ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]
           [(ground 17592186046126) ?ledger|ledger|journal-entries]],
   }
 (db eeleven-url))

(d/q
 '
 {:where
  [(or
    [?ledger :ledger/id]
    [?ledger :ledger/journal-entries]
    [?ledger :ledger/name]
    [?ledger :ledger/tax-entries])
   #_[?ledger :ledger/name ?ledger|ledger|name]
   #_[?ledger :ledger/id ?ledger|ledger|id]
   [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
   (or-join [?ledger ?ledger|ledger|tax-entries]
            [?ledger :ledger/tax-entries ?ledger|ledger|tax-entries]
            (and [?ledger]
                 [(ground -9223372036854775808) ?ledger|ledger|tax-entries]))
   [(datomic.api/entity $ ?ledger) ?ledger-entity]
   [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
   [?ledger|ledger|journal-entries
    :journal-entry/id
    ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]
   [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
   [?ledger|ledger|journal-entries
    :journal-entry/id
    ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]
   [(ground 17592186046126) ?ledger|ledger|journal-entries]],
  :find
  [?ledger
   #_?ledger|ledger|name
   #_?ledger|ledger|id
   ?ledger|ledger|journal-entries
   ?ledger|ledger|tax-entries
   ?ledger|ledger|journal-entries->journal-entry|journal-entry|id],
  }

 (db eeleven-url))

#{[17592186045737 17592186046126 "JE-2879-00-0126"]}

(:ledger/tax-entries (d/entity
                      (db eeleven-url)
                      17592186045737)
                     ::missing)

(take 10 (db eeleven-url))

Long/MIN_VALUE
(d/q
 '{:where
   [(or
     [?journal-entry :journal-entry/base-rate]
     [?journal-entry :journal-entry/currency]
     [?journal-entry :journal-entry/date]
     [?journal-entry :journal-entry/document-date]
     [?journal-entry :journal-entry/document-number]
     [?journal-entry :journal-entry/document-reference]
     [?journal-entry :journal-entry/document-sequence]
     [?journal-entry :journal-entry/document-type]
     [?journal-entry :journal-entry/external-id]
     [?journal-entry :journal-entry/id]
     [?journal-entry :journal-entry/journal-entry-lines]
     [?journal-entry :journal-entry/magic-document]
     [?journal-entry :journal-entry/narration]
     [?journal-entry :journal-entry/number]
     [?journal-entry :journal-entry/reverse-of]
     [?journal-entry :journal-entry/reversed-by]
     [?journal-entry :journal-entry/state])
    [(get-else
      $
      ?journal-entry
      :journal-entry/id
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|id]
    [(get-else
      $
      ?journal-entry
      :journal-entry/base-rate
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|base-rate]
    [(get-else
      $
      ?journal-entry
      :journal-entry/currency
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|currency]
    [(get-else
      $
      ?journal-entry
      :journal-entry/date
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract
      :default
      ?journal-entry|journal-entry|date)
     ?journal-entry|journal-entry|date|default]
    [(get-else
      $
      ?journal-entry
      :journal-entry/document-date
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|document-date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract
      :default
      ?journal-entry|journal-entry|document-date)
     ?journal-entry|journal-entry|document-date|default]
    [(get-else
      $
      ?journal-entry
      :journal-entry/document-number
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|document-number]
    [(get-else
      $
      ?journal-entry
      :journal-entry/document-reference
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|document-reference]
    [(get-else
      $
      ?journal-entry
      :journal-entry/document-sequence
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|document-sequence]
    [(get-else
      $
      ?journal-entry
      :journal-entry/document-type
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|document-type]
    [(get-else
      $
      ?journal-entry
      :journal-entry/external-id
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|external-id]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry
      :journal-entry/journal-entry-lines
      ?journal-entry|journal-entry|journal-entry-lines]
     (and
      [?journal-entry]
      [(ground -9223372036854775808)
       ?journal-entry|journal-entry|journal-entry-lines]))
    [(get-else
      $
      ?journal-entry
      :journal-entry/magic-document
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|magic-document]
    [(get-else
      $
      ?journal-entry
      :journal-entry/narration
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|narration]
    [(get-else
      $
      ?journal-entry
      :journal-entry/number
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|number]
    [(get-else
      $
      ?journal-entry
      :journal-entry/reverse-of
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|reverse-of]
    [(get-else
      $
      ?journal-entry
      :journal-entry/reversed-by
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|reversed-by]
    [(get-else
      $
      ?journal-entry
      :journal-entry/state
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|state]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry
      :journal-entry/journal-entry-lines
      ?journal-entry|journal-entry|journal-entry-lines]
     (and
      [?journal-entry]
      [(ground -9223372036854775808)
       ?journal-entry|journal-entry|journal-entry-lines]))
    [(get-else
      $
      ?journal-entry|journal-entry|journal-entry-lines
      :journal-entry-line/id
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry
      :journal-entry/journal-entry-lines
      ?journal-entry|journal-entry|journal-entry-lines]
     (and
      [?journal-entry]
      [(ground -9223372036854775808)
       ?journal-entry|journal-entry|journal-entry-lines]))
    [(get-else
      $
      ?journal-entry|journal-entry|journal-entry-lines
      :journal-entry-line/id
      :metabase.driver.datomic.query-processor/nil)
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]],
   :find
   [?journal-entry
    ?journal-entry|journal-entry|id
    ?journal-entry|journal-entry|base-rate
    ?journal-entry|journal-entry|currency
    ?journal-entry|journal-entry|date|default
    ?journal-entry|journal-entry|document-date|default
    ?journal-entry|journal-entry|document-number
    ?journal-entry|journal-entry|document-reference
    ?journal-entry|journal-entry|document-sequence
    ?journal-entry|journal-entry|document-type
    ?journal-entry|journal-entry|external-id
    ?journal-entry|journal-entry|journal-entry-lines
    ?journal-entry|journal-entry|magic-document
    ?journal-entry|journal-entry|narration
    ?journal-entry|journal-entry|number
    ?journal-entry|journal-entry|reverse-of
    ?journal-entry|journal-entry|reversed-by
    ?journal-entry|journal-entry|state
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id],
   :select
   [?journal-entry
    ?journal-entry|journal-entry|id
    ?journal-entry|journal-entry|base-rate
    ?journal-entry|journal-entry|currency
    ?journal-entry|journal-entry|date|default
    ?journal-entry|journal-entry|document-date|default
    ?journal-entry|journal-entry|document-number
    ?journal-entry|journal-entry|document-reference
    ?journal-entry|journal-entry|document-sequence
    ?journal-entry|journal-entry|document-type
    ?journal-entry|journal-entry|external-id
    ?journal-entry|journal-entry|journal-entry-lines
    ?journal-entry|journal-entry|magic-document
    ?journal-entry|journal-entry|narration
    ?journal-entry|journal-entry|number
    ?journal-entry|journal-entry|reverse-of
    ?journal-entry|journal-entry|reversed-by
    ?journal-entry|journal-entry|state
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id],
   :with ()}
 (db eeleven-url))


(d/q
 '{:where
   [(or [?checkins :checkins/date] [?checkins :checkins/user_id] [?checkins :checkins/venue_id])
    [(get-else $ ?checkins :checkins/date :metabase.driver.datomic.query-processor/nil) ?checkins|checkins|date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :default ?checkins|checkins|date) ?checkins|checkins|date|default]
    [(get-else $ ?checkins :checkins/user_id :metabase.driver.datomic.query-processor/nil) ?checkins|checkins|user_id]
    [(get-else $ ?checkins :checkins/venue_id :metabase.driver.datomic.query-processor/nil) ?checkins|checkins|venue_id]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :day ?checkins|checkins|date) ?checkins|checkins|date|day]
    [(metabase.driver.datomic.util/lte #inst "2014-12-31T00:00:00.000000000-00:00" ?checkins|checkins|date|day)]
    [(metabase.driver.datomic.util/lte ?checkins|checkins|date|day #inst "2015-01-31T00:00:00.000000000-00:00")]],
   :find [?checkins ?checkins|checkins|date|default ?checkins|checkins|user_id ?checkins|checkins|venue_id],
   :select [?checkins ?checkins|checkins|date|default ?checkins|checkins|user_id ?checkins|checkins|venue_id],
   :with ()}
 (db "datomic:mem:test-data"))

(d/q
 '
 {:where
  [(or [?checkins :checkins/date]
       [?checkins :checkins/user_id]
       [?checkins :checkins/venue_id])

   [(get-else $ ?checkins :checkins/date :metabase.driver.datomic.query-processor/nil)
    ?checkins|checkins|date]

   [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :week ?checkins|checkins|date)
    ?checkins|checkins|date|week]

   [(= #inst "2015-06-21T00:00:00.000000000-00:00" ?checkins|checkins|date|week)]],

  :find [(count ?checkins)],
  :select [(count ?checkins)],
  :with ()}




 (db "datomic:mem:test-data"))

(d/touch
 (d/entity (db "datomic:mem:test-data") :venues/name))

(d/q
 '{:where
   [(or
     [?journal-entry :journal-entry/base-rate]
     [?journal-entry :journal-entry/currency]
     [?journal-entry :journal-entry/date]
     [?journal-entry :journal-entry/document-date]
     [?journal-entry :journal-entry/document-number]
     [?journal-entry :journal-entry/document-reference]
     [?journal-entry :journal-entry/document-sequence]
     [?journal-entry :journal-entry/document-type]
     [?journal-entry :journal-entry/external-id]
     [?journal-entry :journal-entry/id]
     [?journal-entry :journal-entry/journal-entry-lines]
     [?journal-entry :journal-entry/magic-document]
     [?journal-entry :journal-entry/narration]
     [?journal-entry :journal-entry/number]
     [?journal-entry :journal-entry/reverse-of]
     [?journal-entry :journal-entry/reversed-by]
     [?journal-entry :journal-entry/state])
    [(get-else $ ?journal-entry :journal-entry/id ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|id]
    [(get-else $ ?journal-entry :journal-entry/base-rate -9223372036854775808) ?journal-entry|journal-entry|base-rate]
    [(get-else $ ?journal-entry :journal-entry/currency -9223372036854775808) ?journal-entry|journal-entry|currency]
    [(get-else $ ?journal-entry :journal-entry/date #inst "0001-01-01T01:01:01.000-00:00") ?journal-entry|journal-entry|date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :default ?journal-entry|journal-entry|date) ?journal-entry|journal-entry|date|default]
    [(get-else $ ?journal-entry :journal-entry/document-date #inst "0001-01-01T01:01:01.000-00:00") ?journal-entry|journal-entry|document-date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :default ?journal-entry|journal-entry|document-date) ?journal-entry|journal-entry|document-date|default]
    [(get-else $ ?journal-entry :journal-entry/document-number ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|document-number]
    [(get-else $ ?journal-entry :journal-entry/document-reference ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|document-reference]
    [(get-else $ ?journal-entry :journal-entry/document-sequence -9223372036854775808) ?journal-entry|journal-entry|document-sequence]
    [(get-else $ ?journal-entry :journal-entry/document-type -9223372036854775808) ?journal-entry|journal-entry|document-type]
    [(get-else $ ?journal-entry :journal-entry/external-id ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|external-id]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry :journal-entry/journal-entry-lines ?journal-entry|journal-entry|journal-entry-lines]
     (and [?journal-entry] [(ground -9223372036854775808) ?journal-entry|journal-entry|journal-entry-lines]))
    [(get-else $ ?journal-entry :journal-entry/magic-document -9223372036854775808) ?journal-entry|journal-entry|magic-document]
    [(get-else $ ?journal-entry :journal-entry/narration ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|narration]
    [(get-else $ ?journal-entry :journal-entry/number ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|number]
    [(get-else $ ?journal-entry :journal-entry/reverse-of -9223372036854775808) ?journal-entry|journal-entry|reverse-of]
    [(get-else $ ?journal-entry :journal-entry/reversed-by -9223372036854775808) ?journal-entry|journal-entry|reversed-by]
    [(get-else $ ?journal-entry :journal-entry/state -9223372036854775808) ?journal-entry|journal-entry|state]
    [(get-else $ ?journal-entry|journal-entry|journal-entry-lines :journal-entry-line/id ":metabase.driver.datomic.query-processor/nil")
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]
    [(ground 17592186045626) ?journal-entry|journal-entry|document-type]],
   :find
   [?journal-entry
    ?journal-entry|journal-entry|id
    ?journal-entry|journal-entry|base-rate
    ?journal-entry|journal-entry|currency
    ?journal-entry|journal-entry|date|default
    ?journal-entry|journal-entry|document-date|default
    ?journal-entry|journal-entry|document-number
    ?journal-entry|journal-entry|document-reference
    ?journal-entry|journal-entry|document-sequence
    ?journal-entry|journal-entry|document-type
    ?journal-entry|journal-entry|external-id
    ?journal-entry|journal-entry|journal-entry-lines
    ?journal-entry|journal-entry|magic-document
    ?journal-entry|journal-entry|narration
    ?journal-entry|journal-entry|number
    ?journal-entry|journal-entry|reverse-of
    ?journal-entry|journal-entry|reversed-by
    ?journal-entry|journal-entry|state
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]}
 (db eeleven-url)
 )

(d/entity (db eeleven-url) :document-type/general)
#:db{:id }
(user/refer-repl)


(d/q
 '{:where
   [(or
     [?journal-entry :journal-entry/base-rate]
     [?journal-entry :journal-entry/currency]
     [?journal-entry :journal-entry/date]
     [?journal-entry :journal-entry/document-date]
     [?journal-entry :journal-entry/document-number]
     [?journal-entry :journal-entry/document-reference]
     [?journal-entry :journal-entry/document-sequence]
     [?journal-entry :journal-entry/document-type]
     [?journal-entry :journal-entry/external-id]
     [?journal-entry :journal-entry/id]
     [?journal-entry :journal-entry/journal-entry-lines]
     [?journal-entry :journal-entry/magic-document]
     [?journal-entry :journal-entry/narration]
     [?journal-entry :journal-entry/number]
     [?journal-entry :journal-entry/reverse-of]
     [?journal-entry :journal-entry/reversed-by]
     [?journal-entry :journal-entry/state])
    [(get-else $ ?journal-entry :journal-entry/id ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|id]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry :journal-entry/journal-entry-lines ?journal-entry|journal-entry|journal-entry-lines]
     (and [(missing? $ ?journal-entry :journal-entry/journal-entry-lines)]
          [(ground -9223372036854775808) ?journal-entry|journal-entry|journal-entry-lines]))
    [?journal-entry :journal-entry/currency ?journal-entry|journal-entry|currency]
    [(get-else $ ?journal-entry|journal-entry|journal-entry-lines :journal-entry-line/id ":metabase.driver.datomic.query-processor/nil")
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]
    [(ground 17592186045512) ?journal-entry|journal-entry|currency]],
   :find
   [
    ?journal-entry|journal-entry|id
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id],
   }
 (db eeleven-url)
 )
