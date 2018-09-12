(ns metabase.mbql.util
  "Utilitiy functions for working with MBQL queries."
  (:require [clojure
             [string :as str]
             [walk :as walk]]
            [metabase.mbql.schema :as mbql.s]
            [metabase.util :as u]
            [metabase.util.schema :as su]
            [schema.core :as s]))

(s/defn normalize-token :- s/Keyword
  "Convert a string or keyword in various cases (`lisp-case`, `snake_case`, or `SCREAMING_SNAKE_CASE`) to a lisp-cased
  keyword."
  [token :- su/KeywordOrString]
  (-> (u/keyword->qualified-name token)
      str/lower-case
      (str/replace #"_" "-")
      keyword))

(defn mbql-clause?
  "True if `x` is an MBQL clause (a sequence with a keyword as its first arg). (Since this is used by the code in
  `normalize` this handles pre-normalized clauses as well.)"
  [x]
  (and (sequential? x)
       (keyword? (first x))))

(defn is-clause?
  "If `x` an MBQL clause, and an instance of clauses defined by keyword(s) `k-or-ks`?

    (is-clause? :count [:count 10])        ; -> true
    (is-clause? #{:+ :- :* :/} [:+ 10 20]) ; -> true"
  [k-or-ks x]
  (and
   (mbql-clause? x)
   (if (coll? k-or-ks)
     ((set k-or-ks) (first x))
     (= k-or-ks (first x)))))

(defn clause-instances
  "Return a sequence of all the instances of clause(s) in `x`. Like `is-clause?`, you can either look for instances of a
  single clause by passing a single keyword or for instances of multiple clauses by passing a set of keywords. Returns
  `nil` if no instances were found.

    ;; look for :field-id clauses
    (clause-instances :field-id {:query {:filter [:= [:field-id 10] 20]}})
    ;;-> [[:field-id 10]]

    ;; look for :+ or :- clauses
    (clause-instances #{:+ :-} ...)"
  {:style/indent 1}
  [k-or-ks x]
  (let [instances (transient [])]
    (walk/postwalk
     (fn [clause]
       (u/prog1 clause
         (when (is-clause? k-or-ks clause)
           (conj! instances clause))))
     x)
    (seq (persistent! instances))))

(defn replace-clauses
  "Walk a query looking for clauses named by keyword or set of keywords `k-or-ks` and replace them the results of a call
  to `(f clause)`.

    (replace-clauses {:filter [:= [:field-id 10] 100]} :field-id (constantly 200))
    ;; -> {:filter [:= 200 100]}"
  {:style/indent 2}
  [query k-or-ks f]
  (walk/postwalk
   (fn [clause]
     (if (is-clause? k-or-ks clause)
       (f clause)
       clause))
   query))

(defn replace-clauses-in
  "Replace clauses only in a subset of `query`, defined by `keypath`.

    (replace-clauses-in {:filter [:= [:field-id 10] 100], :breakout [:field-id 100]} [:filter] :field-id
      (constantly 200))
    ;; -> {:filter [:= 200 100], :breakout [:field-id 100]}"
  {:style/indent 3}
  [query keypath k-or-ks f]
  (update-in query keypath #(replace-clauses % k-or-ks f)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                       Functions for manipulating queries                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

;; TODO - we should validate the query against the Query schema and the output as well. Flip that on once the schema
;; is locked-in 100%

(s/defn add-filter-clause
  "Add an additional filter clause to an `outer-query`."
  [outer-query :- su/Map, new-clause :- mbql.s/Filter]
  (update-in outer-query [:query :filter] (fn [existing-clause]
                                            (cond
                                              ;; if top-level clause is `:and` then just add the new clause at the end
                                              (is-clause? :and existing-clause)
                                              (conj existing-clause new-clause)

                                              ;; otherwise if we have an existing clause join to new one with an `:and`
                                              existing-clause
                                              [:and existing-clause new-clause]

                                              ;; if we don't have existing clause then new clause is the new top-level
                                              :else
                                              new-clause))))

(s/defn add-filter-clauses
  "Add multiple filter clauses to an `outer-query`. If `new-clauses` is empty or `nil`, this is a no-op."
  [outer-query :- su/Map, new-clauses :- [mbql.s/Filter]]
  (loop [query outer-query, [filter-clause & more] new-clauses]
    (if-not filter-clause
      query
      (recur (add-filter-clause query filter-clause) more))))
