(ns metabase.query-processor.middleware.expand-macros
  "Middleware for expanding `METRIC` and `SEGMENT` 'macros' in *unexpanded* MBQL queries.

   Code in charge of expanding [\"METRIC\" ...] and [\"SEGMENT\" ...] forms in MBQL queries.
   (METRIC forms are expanded into aggregations and sometimes filter clauses, while SEGMENT forms
    are expanded into filter clauses.)

   TODO - this namespace is ancient and written with MBQL '95 in mind, e.g. it is case-sensitive.
   At some point this ought to be reworked to be case-insensitive and cleaned up."
  (:require [clojure.tools.logging :as log]
            [metabase.mbql
             [normalize :as normalize]
             [util :as mbql.u]]
            [metabase.models
             [metric :refer [Metric]]
             [segment :refer [Segment]]]
            [metabase.query-processor
             [interface :as i]
             [util :as qputil]]
            [metabase.util :as u]
            [puppetlabs.i18n.core :refer [tru]]
            [toucan.db :as db]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    SEGMENTS                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- segment-clauses->id->definition [segment-clauses]
  (db/select-id->field :definition Segment, :id [:in (set (map second segment-clauses))]))

(defn- replace-segment-clauses [outer-query segment-id->definition]
  (mbql.u/replace-clauses-in outer-query [:query :filter] :segment
    (fn [[_ segment-id]]
      (or (:filter (segment-id->definition segment-id))
          (throw (IllegalArgumentException. (str (tru "Segment {0} does not exist, or is invalid." segment-id))))))))

(defn- expand-segments [{{filters :filter} :query, :as outer-query}]
  (if-let [segments (mbql.u/clause-instances :segment filters)]
    (replace-segment-clauses outer-query (segment-clauses->id->definition segments))
    outer-query))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    METRICS                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn ga-metric?
  "Is this metric clause not a Metabase Metric, but rather a GA one? E.g. something like [metric ga:users]. We want to
   ignore those because they're not the same thing at all as MB Metrics and don't correspond to objects in our
   application DB."
  [[_ id]]
  (boolean
   (when ((some-fn string? keyword?) id)
     (re-find #"^ga(id)?:" (name id)))))

(defn- metric-clauses->definitions [metric-clauses]
  (db/select-field :definition Metric, :id [:in (set (map second metric-clauses))]))

(defn- add-metrics-clauses
  "Add appropriate `filter` and `aggregation` clauses for a sequence of Metrics.

    (add-metrics-clauses {:query {}} [[:metric 10]])
    ;; -> {:query {:aggregation [[:count]], :filter [:= [:field-id 10] 20]}}"
  [query metric-defs]
  (let [filters      (filter identity (map :filter metric-defs))
        aggregations (filter identity (mapcat :aggregation metric-defs))]
    (-> query
        (mbql.u/add-filter-clauses filters)
        (update-in [:query :aggregation] concat aggregations))))

(defn- remove-metrics
  "Remove any `:metric` clauses (excluding GA) from the `:aggregation` clause."
  [outer-query]
  (-> (mbql.u/replace-clauses-in outer-query [:query :aggregation] :metric
        (fn [metric]
          (when (ga-metric? metric)
            metric)))
      (update-in [:query :aggregation] (partial filter identity))))

(defn- metrics
  "Return a sequence of any (non-GA) `:metric` MBQL clauses in `query`."
  [{{aggregations :aggregation} :query, :as query}]
  (seq (filter (complement ga-metric?) (mbql.u/clause-instances :metric aggregations))))

(defn- expand-metrics [query]
  (if-let [metrics (metrics query)]
    (add-metrics-clauses (remove-metrics query) (metric-clauses->definitions metrics))
    query))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   MIDDLEWARE                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- expand-metrics-and-segments
  "Expand the macros (`segment`, `metric`) in a `query`."
  [query]
  (-> query
      expand-metrics
      expand-segments
      ;; normalize the query again just in case we did something like accidentally added empty ag or filter clauses
      normalize/normalize))


(defn- expand-macros* [query]
  (if-not (qputil/mbql-query? query)
    query
    (u/prog1 (expand-metrics-and-segments query)
      (when (and (not i/*disable-qp-logging*)
                 (not= <> query))
        (log/debug (u/format-color 'cyan "\n\nMACRO/SUBSTITUTED: %s\n%s" (u/emoji "😻") (u/pprint-to-str <>)))))))

(defn expand-macros
  "Middleware that looks for `METRIC` and `SEGMENT` macros in an unexpanded MBQL query and substitute the macros for
  their contents."
  [qp] (comp qp expand-macros*))
