(ns passim.utils
  (:require [clojure.string :as s]))

(defn doc-id-parts
  [docid]
  (s/split docid #"[_/]" 2))

(defn doc-series
  [docid]
  (first (doc-id-parts docid)))

(defn doc-date
  [docid]
  (second (doc-id-parts docid)))

(defn doc-series-pair
  [[no id]]
  [(Integer/parseInt no) (doc-series id)])

(defn space-count
  [^String s]
  (count (re-seq #" " s)))

(defn spacel?
  [^String s]
  (= " " (subs s 0 1)))

(defn join-alnum-tokens
  [toks]
  (s/replace (s/join " " toks) #"[^a-zA-Z0-9 ]" "#"))

(def match-matrix (jaligner.matrix.MatrixGenerator/generate 2 -1))

(defrecord Alignment [sequence1 sequence2 start1 start2 end1 end2])

(defn maxer
  [f]
  (fn [a b] (< (f a) (f b)) b a))

(defn alignment-stats
  [^Alignment alg]
  (let [pairs (partition 2 (interleave (:sequence1 alg) (:sequence2 alg)))
        gaps (concat (re-seq #"\-+" (:sequence1 alg))
                     (re-seq #"\-+" (:sequence2 alg)))
        nmatches (count (filter (partial apply =) pairs))
        ngaps (count gaps)]
    {:matches nmatches
     :gaps ngaps
     :swscore (+ (* 2 nmatches)
        (* -1 (count (filter (fn [[a b]] (and (not= a b) (not= a \-) (not= b \-))) pairs)))
        (* -5 ngaps)
        (* -0.5 (reduce + (map (comp dec count) gaps))))}
    ))
