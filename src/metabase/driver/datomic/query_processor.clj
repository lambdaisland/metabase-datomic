(ns metabase.driver.datomic.query-processor)


;; native
'{:database 3,
  :type :query,
  :query
  {:source-table 8,
   :fields
   [[:field-id 72]
    [:field-id 66]
    [:field-id 76]
    [:field-id 67]
    [:field-id 70]
    [:field-id 74]
    [:field-id 65]
    [:field-id 69]
    [:field-id 73]
    [:field-id 75]
    [:field-id 68]
    [:field-id 64]
    [:field-id 71]],
   :limit 2000},
  :constraints {:max-results 10000, :max-results-bare-rows 2000},
  :info
  {:executed-by 1,
   :context :ad-hoc,
   :nested? false,
   :query-hash "#object[[B 0x8a8c1c1 [B@8a8c1c1]",
   :query-type MBQL},
  :results-promise "#promise[{:status :pending, :val nil} 0x1d8c6198]",
  :driver :datomic,
  :settings {}}

;; MBQL
'{:database 3,
  :type :query,
  :query
  {:source-table 8,
   :fields
   [[:field-id 72]
    [:field-id 66]
    [:field-id 76]
    [:field-id 67]
    [:field-id 70]
    [:field-id 74]
    [:field-id 65]
    [:field-id 69]
    [:field-id 73]
    [:field-id 75]
    [:field-id 68]
    [:field-id 64]
    [:field-id 71]],
   :limit 2000},
  :constraints {:max-results 10000, :max-results-bare-rows 2000},
  :info
  {:executed-by 1,
   :context :ad-hoc,
   :nested? false,
   :query-hash "#object[[B 0x8a8c1c1 [B@8a8c1c1]",
   :query-type MBQL},
  :driver :datomic,
  :settings {},
  :native {:query {:find [(pull ?e :*)], :where [[?e :artist/name]]}}}
