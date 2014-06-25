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
  (s/join " " toks))

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

(defn var-doc
  [v]
  (:doc (meta v)))

(defn loc-scale
  [in]
  (int (/ in 4)))

(defn loc-url
  [base text]
  (let [raw
        (->> text
             (re-seq #"<w p=\"([^\"]+)\" c=\"(\d+),(\d+),(\d+),(\d+)\"")
             (map (fn [[_ k & vals]] [k (mapv #(Integer/parseInt %) vals)])))
        p (ffirst raw)
        dat (map second (filter #(= p (first %)) raw))
        x1 (->> dat (map first) (reduce min) loc-scale)
        y1 (->> dat (map second) (reduce min) loc-scale)
        ;; What idiot put height before width in camun.pl?!?!?
        x2 (->> dat (map (fn [[x y h w]] (+ x w))) (reduce max) loc-scale)
        y2 (->> dat (map (fn [[x y h w]] (+ y h))) (reduce max) loc-scale)]
    (format "%s/%s/print/image_%dx%d_from_%d%%2C%d_to_%d%%2C%d/" base p
            600 600 x1 y1 x2 y2)))
