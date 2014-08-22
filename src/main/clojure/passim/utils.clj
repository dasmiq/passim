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

(defn hapax-positions
  "Returns map of hapax terms to their positions in sequence"
  [s]
  (->> s
       (map-indexed vector)
       (reduce
        (fn [map [pos word]]
          (update-in map [word] #(if (nil? %) pos -1)))
        {})
       (remove #(< (second %) 0))
       (into {})))

(defn find-hapax-anchors
  [terms1 terms2]
  (->>
   (merge-with vector (hapax-positions terms1) (hapax-positions terms2))
   vals
   (filter vector?)
   sort
   vec))

(defn- find-match-anchors
  [matches]
  (let [res (->> matches
                 (map
                  (fn [[gram df p1 p2]]
                    (when (= 1 (count p1) (count p2))
                      (vector (first p1) (first p2)))))
                 (remove nil?)
                 sort
                 vec)]
    (when (not-empty res) res)))

(defn largest-binary-search
  [pred low high default]
  (if (< high low)
    default
    (let [mid (+ low (int (/ (- high low) 2)))]
      (cond (= mid low) (cond (pred high) high
                              (pred low) low
                              :else default)
            (pred mid) (recur pred mid high default)
            :else (recur pred low (dec mid) default)))))

(defn longest-increasing-subsequence
  "Output indices of the LIS"
  [coll]
  (when (seq coll)
    (let [s (vec coll)
          n (count s)]
      (loop [L 0
             i 1
             M (vec (take (inc n) (repeat 0)))
             P (vec (take (inc n) (repeat 0)))]
        (if (<= i n)
          (let [cur (s (dec i))
                j (largest-binary-search (fn [x] (< (s (dec (M (dec x)))) cur)) 1 L 0)
                newP (if (> j 0) (assoc P (dec i) (M (dec j))) P)]
            (if (or (= j L) (< cur (s (dec (M j)))))
              (recur (long (max L (inc j)))
                     (inc i)
                     (assoc M j i)
                     newP)
              (recur L
                     (inc i)
                     M
                     newP)))
          ;; Traceback
          (loop [res (list (dec (M (dec L))))]
            (let [prev (P (first res))]
              (if (> prev 0)
                (recur (cons (dec prev) res))
                (seq res)))))))))

(defn swg-align
  [w1 w2]
  (let [c1 (join-alnum-tokens w1)
        c2 (join-alnum-tokens w2)
        alg (jaligner.SmithWatermanGotoh/align
             (jaligner.Sequence. c1) (jaligner.Sequence. c2) match-matrix 5 0.5)
        out1 (String. (.getSequence1 alg))
        out2 (String. (.getSequence2 alg))
        os1 (.getStart1 alg)
        os2 (.getStart2 alg)
        sword1 (+ (space-count (subs c1 0 os1))
                  (if (spacel? out1) 1 0))
        sword2 (+ (space-count (subs c2 0 os2))
                  (if (spacel? out2) 1 0))]
    (Alignment. out1 out2
                sword1 sword2
                    (+ sword1 1 (space-count (s/trim out1)))
                    (+ sword2 1 (space-count (s/trim out2))))))

(defn- align-words
  [start end w1 w2 gap-words]
  (let [[s1 s2] (if (not-empty start)
                  start
                  [(max 0 (- (first end) gap-words))
                   (max 0 (- (second end) gap-words))])
        [e1 e2] (if (not-empty end)
                  end
                  [(min (dec (count w1)) (+ (first start) gap-words))
                   (min (dec (count w2)) (+ (second start) gap-words))])
        c1 (join-alnum-tokens (subvec w1 s1 (inc e1)))
        c2 (join-alnum-tokens (subvec w2 s2 (inc e2)))
        alg (jaligner.SmithWatermanGotoh/align
             (jaligner.Sequence. c1) (jaligner.Sequence. c2) match-matrix 5 0.5)
        out1 (String. (.getSequence1 alg))
        out2 (String. (.getSequence2 alg))
        os1 (.getStart1 alg)
        os2 (.getStart2 alg)]
    ;; (prn [os1 os2 out1 out2])
    (when (and (or (empty? start) (= 0 os1 os2))
               ;; Probably should count alignment without hyphen gaps
               (or (empty? end) (and (<= (count c1) (+ os1 (count out1)))
                                     (<= (count c2) (+ os2 (count out2))))))
      (let [sword1 (+ s1 (space-count (subs c1 0 os1))
                      (if (spacel? out1) 1 0))
            sword2 (+ s2 (space-count (subs c2 0 os2))
                      (if (spacel? out2) 1 0))]
        (Alignment. out1 out2
                    sword1 sword2
                    (+ sword1 1 (space-count (s/trim out1)))
                    (+ sword2 1 (space-count (s/trim out2))))))))

(defn- increasing-matches
  [matches]
  (let [lis (longest-increasing-subsequence (map second matches))]
    (mapv (partial get matches) lis)))

(defn- trim-gram
  [^Alignment alg ^long gram]
  (let [m1 (dec gram)]
    (assoc alg
      :end1 (- (:end1 alg) m1)
      :end2 (- (:end2 alg) m1)
      :sequence1 (s/join " " (drop-last gram (s/split (:sequence1 alg) #" ")))
      :sequence2 (s/join " " (drop-last gram (s/split (:sequence2 alg) #" "))))))

(defn best-passages
  [w1 w2 matches gram]
  (when-let [anch (find-match-anchors matches)]
    (let [
          ;; (find-hapax-anchors (partition gram 1 w1) (partition gram 1 w2))
          ;; (->> matches vals first rest (map second) (map first) vec vector))
          inc-anch (increasing-matches anch)
          gap-words 100
          add-gram (partial + (dec gram))
          middles (mapcat
                   (fn [[[s1 s2] [e1 e2]]]
                     (if (> (max (- e1 s1) (- e2 s2)) 1)
                       (if (and (> (- e1 s1) gap-words) (> (- e2 s2) gap-words))
                         (list (align-words [s1 s2] [] w1 w2 gap-words)
                               nil
                               ;; shorten gap-words?
                               (align-words [] [e1 e2] w1 w2 gap-words))
                         (if-let [gap
                                  (align-words [s1 s2] [(add-gram e1) (add-gram e2)]
                                               w1 w2 gap-words)]
                           ;; Remove tacked-on trailing words
                           (list (trim-gram gap gram))
                           (list
                            (Alignment. (s/join " " (subvec w1 s1 (+ s1 gram)))
                                        (s/join " " (subvec w2 s2 (+ s2 gram)))
                                        s1 s2 (+ s1 gram) (+ s2 gram))
                            nil)))
                       (list (Alignment. (w1 s1) (w2 s2) s1 s2 (inc s1) (inc s2)))))
                   (partition 2 1 inc-anch))
          ;; removing trailing tacked-on words
          leading (when-let
                      [res (align-words [] (mapv add-gram (first inc-anch)) w1 w2 gap-words)]
                    (list (trim-gram res gram)))
          ;; Problem: not properly anchored at the left edge
          trailing (when-let
                       [res (align-words (nth inc-anch (dec (count inc-anch))) [] w1 w2 gap-words)]
                     (list res))]
      ;; (prn inc-anch)
      ;; (prn leading middles trailing)
      (remove nil?
              (for [span (partition-by nil? (concat leading middles trailing))]
                (when (first span)
                  (let [vspan (vec span)
                        head (first vspan)
                        tail (peek vspan)]
                    (Alignment.
                     (s/join " " (map (comp s/trim :sequence1) vspan))
                     (s/join " " (map (comp s/trim :sequence2) vspan))
                     (:start1 head) (:start2 head) (:end1 tail) (:end2 tail)))))))))
