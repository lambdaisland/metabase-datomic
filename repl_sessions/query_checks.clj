(ns query-checks
  (:require [datomic.api :as d]
            [clojure.set :as set]))

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
    (or-join
     [?journal-entry ?96910]
     [?journal-entry :journal-entry/journal-entry-lines ?96910]
     (and [(missing? $ ?journal-entry :journal-entry/journal-entry-lines)] [(ground -9223372036854775808) ?96910]))
    [(get-else $ ?96910 :journal-entry-line/account -9223372036854775808) ?journal-entry|journal-entry|account]
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
    [(get-else $ ?journal-entry :journal-entry/id ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|id]
    (or-join
     [?journal-entry ?journal-entry|journal-entry|journal-entry-lines]
     [?journal-entry :journal-entry/journal-entry-lines ?journal-entry|journal-entry|journal-entry-lines]
     (and [(missing? $ ?journal-entry :journal-entry/journal-entry-lines)] [(ground -9223372036854775808) ?journal-entry|journal-entry|journal-entry-lines]))
    [(get-else $ ?journal-entry :journal-entry/magic-document -9223372036854775808) ?journal-entry|journal-entry|magic-document]
    [(get-else $ ?journal-entry :journal-entry/narration ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|narration]
    [(get-else $ ?journal-entry :journal-entry/number ":metabase.driver.datomic.query-processor/nil") ?journal-entry|journal-entry|number]
    [(get-else $ ?journal-entry :journal-entry/reverse-of -9223372036854775808) ?journal-entry|journal-entry|reverse-of]
    [(get-else $ ?journal-entry :journal-entry/reversed-by -9223372036854775808) ?journal-entry|journal-entry|reversed-by]
    [(get-else $ ?journal-entry :journal-entry/state -9223372036854775808) ?journal-entry|journal-entry|state]
    [(get-else $ ?journal-entry|journal-entry|journal-entry-lines :journal-entry-line/id ":metabase.driver.datomic.query-processor/nil")
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id]],
   :find
   [?journal-entry
    ?journal-entry|journal-entry|account
    ?journal-entry|journal-entry|base-rate
    ?journal-entry|journal-entry|currency
    ?journal-entry|journal-entry|date|default
    ?journal-entry|journal-entry|document-date|default
    ?journal-entry|journal-entry|document-number
    ?journal-entry|journal-entry|document-reference
    ?journal-entry|journal-entry|document-sequence
    ?journal-entry|journal-entry|document-type
    ?journal-entry|journal-entry|external-id
    ?journal-entry|journal-entry|id
    ?journal-entry|journal-entry|journal-entry-lines
    ?journal-entry|journal-entry|magic-document
    ?journal-entry|journal-entry|narration
    ?journal-entry|journal-entry|number
    ?journal-entry|journal-entry|reverse-of
    ?journal-entry|journal-entry|reversed-by
    ?journal-entry|journal-entry|state
    ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id],
   :select
   [(metabase.driver.datomic.query-processor/field ?journal-entry {:database_type "db.type/ref", :base_type :type/PK, :special_type :type/PK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|account {:database_type "metabase.driver.datomic/path", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|base-rate {:database_type "db.type/bigdec", :base_type :type/Decimal, :special_type :type/Decimal})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|currency {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/datetime ?journal-entry|journal-entry|date :default)
    (metabase.driver.datomic.query-processor/datetime ?journal-entry|journal-entry|document-date :default)
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|document-number {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|document-reference {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|document-sequence {:database_type "db.type/long", :base_type :type/Integer, :special_type :type/Integer})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|document-type {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|external-id {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|id {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|journal-entry-lines {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|magic-document {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|narration {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|number {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|reverse-of {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|reversed-by {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?journal-entry|journal-entry|state {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field
     ?journal-entry|journal-entry|journal-entry-lines->journal-entry-line|journal-entry-line|id
     {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})],
   :with ()}
 (db eeleven-url))


(d/q
 '{
   :find
   [?ledger ?ledger|ledger|id ?ledger|ledger|journal-entries ?ledger|ledger|name ?ledger|ledger|tax-entries ?ledger|ledger|journal-entries->journal-entry|journal-entry|id],

   :where
   [(or [?ledger :ledger/id]
        [?ledger :ledger/journal-entries]
        [?ledger :ledger/name]
        [?ledger :ledger/tax-entries])

    [(get-else $ ?ledger :ledger/id ":metabase.driver.datomic.query-processor/nil") ?ledger|ledger|id]

    (or-join [?ledger ?ledger|ledger|journal-entries]
             [?ledger :ledger/journal-entries ?ledger|ledger|journal-entries]
             (and [(missing? $ ?ledger :ledger/journal-entries)]
                  [(ground -9223372036854775808) ?ledger|ledger|journal-entries]))

    [(get-else $ ?ledger :ledger/name ":metabase.driver.datomic.query-processor/nil") ?ledger|ledger|name]

    (or-join [?ledger ?ledger|ledger|tax-entries]
             [?ledger :ledger/tax-entries ?ledger|ledger|tax-entries]
             (and [(missing? $ ?ledger :ledger/tax-entries)]
                  [(ground -9223372036854775808) ?ledger|ledger|tax-entries]))

    (or-join [?ledger ?journal-entry]
             [?ledger :ledger/journal-entries ?journal-entry]
             (and [(missing? $ ?ledger :ledger/journal-entries)]
                  [(ground -9223372036854775808) ?journal-entry]))

    [(get-else $ ?journal-entry :journal-entry/id ":metabase.driver.datomic.query-processor/nil") ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]

    [(ground "JE-2879-00-0055") ?ledger|ledger|journal-entries->journal-entry|journal-entry|id]],
   }
 (db eeleven-url))


(d/q
 '{:in [$ %],
   :where
   [(or
     [?account :account/bank-account-name]
     [?account :account/bank-account-number]
     [?account :account/category]
     [?account :account/children]
     [?account :account/closed?]
     [?account :account/contact-cards]
     [?account :account/currency]
     [?account :account/description]
     [?account :account/expense-category]
     [?account :account/id]
     [?account :account/name]
     [?account :account/number]
     [?account :account/subtype]
     [?account :account/type])
    [(get-else $ ?account :account/currency -9223372036854775808) ?account|account|currency]
    [(get-else $ ?account|account|currency
               :currency/enabled?
               :metabase.driver.datomic.query-processor/nil)
     ?account|account|currency->currency|currency|enabled?]],
   :find [(count ?account)
          ?account|account|currency->currency|currency|enabled?],
   :order-by
   [[:desc (count ?account)]
    [:asc (metabase.driver.datomic.query-processor/field ?account|account|currency->currency|currency|enabled? {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})]],
   :select
   [(metabase.driver.datomic.query-processor/field ?account|account|currency->currency|currency|enabled? {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK}) (count ?account)],
   }
 (db eeleven-url))

(supers (class (:tail (d/log (conn eeleven-url)))))

(let [log (:tail (d/log (conn eeleven-url)))]
  (into {} (map (juxt identity #(class (% log)))) (keys log)))

(:txes (:tail (d/log (conn eeleven-url))))

(inc 1)

(map (comp d/touch (partial d/entity (db eeleven-url)) d/t->tx  :t) (d/tx-range (d/log (conn eeleven-url)) nil nil))
{:db/id 13194139535416, :db/txInstant #inst "2019-04-17T11:46:55.577-00:00", :conformity/conformed-norms :elvn.db-migration/init-sample, :conformity/conformed-norms-index 1, :tx/tenant #:db{:id 17592186046521}}


(count (seq (d/seek-datoms (db eeleven-url) :eavt 13194139535416)))

(d/q '[:find ?eid
       :where [?eid :tenant/id]]
     (d/filter

      (db eeleven-url)

      (fn [db ^datomic.Datom datom]
        (let [tx-tenant (get-in (d/entity db (.tx datom)) [:tx/tenant :db/id])]
          (or (nil? tx-tenant) (= 17592186046521 tx-tenant))))
      ))
;; => #{[17592186046521]}
;; => #{[17592186046521] [17592186045660] [17592186046526]}


(println "{:in [$ %],\n :where\n [(or\n   [?release :release/artists]\n   [?release :release/artistCredit]\n   [?release :release/year]\n   [?release :release/name]\n   [?release :release/status]\n   [?release :release/language]\n   [?release :release/barcode]\n   [?release :release/month]\n   [?release :release/day]\n   [?release :release/gid]\n   [?release :release/media]\n   [?release :release/abstractRelease]\n   [?release :release/country]\n   [?release :release/packaging]\n[?release :release/labels]\n   [?release :release/script])\n  [(get-else $ ?release :release/year -9223372036854775808) ?release|release|year]\n  [(metabase.driver.datomic.util/lte 1968.8447095282804 ?release|release|year)]\n  [(metabase.driver.datomic.util/lte ?release|release|year 1971.1973666493718)]],\n :find [?release|release|year (count ?release)],\n :order-by [[:asc (metabase.driver.datomic.query-processor/field ?release|release|year {:database_type \"db.type/long\", :base_type :type/Integer, :special_type :type/Integer})]],\n :select [(metabase.driver.datomic.query-processor/field ?release|release|year {:database_type \"db.type/long\", :base_type :type/Integer, :special_type :type/Integer}) (count ?release)],\n :with ()}\n")

(d/q
 '{:in [$]
   :where
   [(or
     [?release :release/artists]
     [?release :release/artistCredit]
     [?release :release/year]
     [?release :release/name]
     [?release :release/status]
     [?release :release/language]
     [?release :release/barcode]
     [?release :release/month]
     [?release :release/day]
     [?release :release/gid]
     [?release :release/media]
     [?release :release/abstractRelease]
     [?release :release/country]
     [?release :release/packaging]
     [?release :release/labels]
     [?release :release/script])
    [(get-else $ ?release :release/year -9223372036854775808) ?release|release|year]
    [(metabase.driver.datomic.util/lte 1968.8447095282804 ?release|release|year)]
    [(metabase.driver.datomic.util/lte ?release|release|year 1971.1973666493718)]],
   :find [?release|release|year (count ?release)],
   :order-by [[:asc (metabase.driver.datomic.query-processor/field ?release|release|year {:database_type "db.type/long", :base_type :type/Integer, :special_type :type/Integer})]],
   :select [(metabase.driver.datomic.query-processor/field ?release|release|year {:database_type "db.type/long", :base_type :type/Integer, :special_type :type/Integer}) (count ?release)],
   :with ()}
 (db))


(d/q
 '{
   :where
   [(or
     [?bank-statement :bank-statement/account]
     [?bank-statement :bank-statement/bank-statement-lines]
     [?bank-statement :bank-statement/date]
     [?bank-statement :bank-statement/document]
     [?bank-statement :bank-statement/id])
    [(get-else $ ?bank-statement :bank-statement/account -9223372036854775808) ?bank-statement|bank-statement|account]
    (or-join
     [?bank-statement ?bank-statement|bank-statement|bank-statement-lines]
     [?bank-statement :bank-statement/bank-statement-lines ?bank-statement|bank-statement|bank-statement-lines]
     (and [(missing? $ ?bank-statement :bank-statement/bank-statement-lines)] [(ground -9223372036854775808) ?bank-statement|bank-statement|bank-statement-lines]))
    #_[?bank-statement|bank-statement|company :company/bank-statements ?bank-statement]

    (or-join [?bank-statement|bank-statement|company ?bank-statement]
             [?bank-statement|bank-statement|company :company/bank-statements ?bank-statement]
             (and (not [_ :company/bank-statements ?bank-statement])
                  [(ground -9223372036854775808) ?bank-statement|bank-statement|company]))

    [(get-else $ ?bank-statement :bank-statement/date #inst "0001-01-01T01:01:01.000-00:00") ?bank-statement|bank-statement|date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :default ?bank-statement|bank-statement|date) ?bank-statement|bank-statement|date|default]
    [(get-else $ ?bank-statement :bank-statement/document -9223372036854775808) ?bank-statement|bank-statement|document]
    [(get-else $ ?bank-statement :bank-statement/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|id]
    [(get-else $ ?bank-statement|bank-statement|account :account/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|account->account|account|id]
    [(get-else $ ?bank-statement|bank-statement|document :file/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|document->file|file|id]],
   :find
   [?bank-statement
    ?bank-statement|bank-statement|company
    ?bank-statement|bank-statement|date|default
    ?bank-statement|bank-statement|document
    ?bank-statement|bank-statement|id
    ?bank-statement|bank-statement|account->account|account|id
    ?bank-statement|bank-statement|document->file|file|id],
   :select
   [(metabase.driver.datomic.query-processor/field ?bank-statement {:database_type "db.type/ref", :base_type :type/PK, :special_type :type/PK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|account {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|bank-statement-lines {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|company {:database_type "metabase.driver.datomic/path", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/datetime ?bank-statement|bank-statement|date :default)
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|document {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|id {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|account->account|account|id {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|document->file|file|id {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})],
   :order-by [[:asc (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|bank-statement-lines {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})]],
   :with ()}
 (db eeleven-url))

(d/q
 '{:where
   [(or [?bank-statement :bank-statement/account]
        [?bank-statement :bank-statement/bank-statement-lines]
        [?bank-statement :bank-statement/date]
        [?bank-statement :bank-statement/document]
        [?bank-statement :bank-statement/id])
    [(get-else $ ?bank-statement :bank-statement/account -9223372036854775808) ?bank-statement|bank-statement|account]
    (or-join [?bank-statement ?bank-statement|bank-statement|bank-statement-lines]
             [?bank-statement :bank-statement/bank-statement-lines ?bank-statement|bank-statement|bank-statement-lines]
             (and [(missing? $ ?bank-statement :bank-statement/bank-statement-lines)] [(ground -9223372036854775808) ?bank-statement|bank-statement|bank-statement-lines]))
    (or-join [?bank-statement|bank-statement|company ?bank-statement]
             [?bank-statement|bank-statement|company :company/bank-statements ?bank-statement]
             (and (not [_ :company/bank-statements ?bank-statement]) [(ground -9223372036854775808) ?bank-statement|bank-statement|company]))
    [(get-else $ ?bank-statement :bank-statement/date #inst "0001-01-01T01:01:01.000-00:00") ?bank-statement|bank-statement|date]
    [(metabase.driver.datomic.query-processor/date-trunc-or-extract-some :default ?bank-statement|bank-statement|date) ?bank-statement|bank-statement|date|default]
    [(get-else $ ?bank-statement :bank-statement/document -9223372036854775808) ?bank-statement|bank-statement|document]
    [(get-else $ ?bank-statement :bank-statement/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|id]
    [(get-else $ ?bank-statement|bank-statement|account :account/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|account->account|account|id]
    [(get-else $ ?bank-statement|bank-statement|document :file/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|document->file|file|id]
    [(get-else $ ?bank-statement :bank-statement/company :metabase.driver.datomic.query-processor/nil) ?bank-statement|bank-statement|company]
    [(get-else $ ?bank-statement|bank-statement|company :company/id ":metabase.driver.datomic.query-processor/nil") ?bank-statement|bank-statement|company->company|company|id]
    [(ground "SG-01") ?bank-statement|bank-statement|company->company|company|id]],
   :find
   [?bank-statement
    ?bank-statement|bank-statement|account
    ?bank-statement|bank-statement|bank-statement-lines
    ?bank-statement|bank-statement|company
    ?bank-statement|bank-statement|date|default
    ?bank-statement|bank-statement|document
    ?bank-statement|bank-statement|id
    ?bank-statement|bank-statement|account->account|account|id
    ?bank-statement|bank-statement|document->file|file|id],
   :select
   [(metabase.driver.datomic.query-processor/field ?bank-statement {:database_type "db.type/ref", :base_type :type/PK, :special_type :type/PK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|account {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|bank-statement-lines {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|company {:database_type "metabase.driver.datomic/path", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/datetime ?bank-statement|bank-statement|date :default)
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|document {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|id {:database_type "db.type/string", :base_type :type/Text, :special_type :type/Text})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|account->account|account|id {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})
    (metabase.driver.datomic.query-processor/field ?bank-statement|bank-statement|document->file|file|id {:database_type "db.type/ref", :base_type :type/FK, :special_type :type/FK})],
   }
 (db eeleven-url))

;;;;;;;;;;;;;;;;;;;;;;;;
(user/refer-repl)
