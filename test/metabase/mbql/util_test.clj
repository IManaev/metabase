(ns metabase.mbql.util-test
  (:require [expectations :refer :all]
            [metabase.mbql.util :as mbql.u]))

(expect
  [[:field-id 10]
   [:field-id 20]]
  (mbql.u/clause-instances :field-id {:query {:filter [:=
                                                       [:field-id 10]
                                                       [:field-id 20]]}}))

(expect
  [[:field-id 10]
   [:field-id 20]]
  (mbql.u/clause-instances #{:field-id :+ :-}
    {:query {:filter [:=
                      [:field-id 10]
                      [:field-id 20]]}}))
