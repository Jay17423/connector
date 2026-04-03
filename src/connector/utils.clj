(ns connector.utils)

(defn df->json
  "Converts Spark DataFrame into JSON preview (first 5 rows)."
  [df]
  {:pre [(some? df)]}
  (let [cols (vec (.fieldNames (.schema df)))
        iter (.toLocalIterator df)]
    (loop [rows [] cnt 0]
      (if (or (= cnt 5) (not (.hasNext iter)))
        rows
        (let [row (.next iter)
              parsed (into {} (map-indexed
                               (fn [idx col]
                                 [(keyword col) (.get row idx)])
                               cols))]
          (recur (conj rows parsed) (inc cnt)))))))