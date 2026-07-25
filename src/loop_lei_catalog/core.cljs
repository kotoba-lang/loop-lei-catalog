(ns loop-lei-catalog.core
  "Continuous orchestrator for the cloud-itonami-lei corporate catalog:
  observe -> evaluate -> decide -> act -> record-evidence.

  Role boundary (loop-ux-kaizen `resources/repository-rules.edn`): a `loop-*`
  repo runs cycles and must NOT own domain scoring truth. The rubric -- what
  the axes are, how they are weighted, what counts as unmeasured -- lives in
  the `catalog-maturity` library and is tested there. This namespace only
  observes real state, asks that library what is weakest, runs a bounded
  amount of work, and writes down what actually happened.

  Observation is a live SQL read against the D1 catalog. It is never estimated
  and never carried over from a previous cycle: an unreachable database makes
  the cycle FAIL rather than produce a plan from stale numbers, because a plan
  built on last week's coverage would look identical to one built on today's.

  Every action shells out to the existing superproject scripts rather than
  reimplementing acquisition or discovery here. Those scripts are the tested
  path, they already refuse to fabricate data or bypass bot checks, and
  duplicating them inside a scheduler is how two implementations start to
  drift."
  (:require ["fs" :as fs]
            ["path" :as path]
            ["child_process" :refer [execSync]]
            [clojure.string :as str]
            [cljs.reader :as edn]
            [catalog-maturity.core :as cm]))

(def ^:private db "cloud-itonami-lei-catalog")

(defn- repo-root
  "The superproject root, taken from LEI_LOOP_ROOT or the current directory.
  Not hardcoded and not derived from this file's location: the loop is meant
  to be runnable from a checkout anywhere, including a CI workspace, and the
  taxonomy forbids binding a loop-* repo to one provider's layout."
  []
  (or (aget js/process.env "LEI_LOOP_ROOT") (.cwd js/process)))

;; ── observe ─────────────────────────────────────────────────────────────────

(defn- d1-json [sql]
  (let [out (execSync (str "npx wrangler d1 execute " db " --remote -y --json --command " (pr-str sql))
                      #js {:encoding "utf8" :maxBuffer (* 40 1024 1024) :stdio "pipe"
                           :cwd (repo-root)})
        i (.indexOf out "[")]
    (when (neg? i) (throw (js/Error. (str "no JSON in wrangler output: " (subs out 0 200)))))
    (js->clj (js/JSON.parse (subs out i)) :keywordize-keys true)))

(defn- rows [sql] (get-in (first (d1-json sql)) [:results]))

(defn observe
  "Live counts from D1. Provenance is counted strictly: a document needs BOTH
  a retrieval timestamp and a content hash, since either alone leaves the
  archived text uncheckable."
  []
  (let [agg (first (rows (str "SELECT "
                              "(SELECT COUNT(*) FROM company) AS companies, "
                              "(SELECT COUNT(*) FROM tos_doc) AS docs, "
                              "(SELECT COUNT(DISTINCT lei) FROM tos_doc) AS with_doc, "
                              "(SELECT COUNT(*) FROM tos_doc WHERE retrieved_at IS NOT NULL AND sha256 IS NOT NULL) AS docs_prov, "
                              "(SELECT COUNT(*) FROM company WHERE contact_email IS NOT NULL OR inquiry_form_url IS NOT NULL) AS reachable")))
        by-country (rows "SELECT country, COUNT(*) AS n FROM company WHERE country IS NOT NULL GROUP BY country")]
    {:companies (:companies agg)
     :docs (:docs agg)
     :with-doc (:with_doc agg)
     :docs-with-provenance (:docs_prov agg)
     :reachable (:reachable agg)
     :country-counts (into {} (map (juxt :country :n)) by-country)}))

;; ── evaluate ────────────────────────────────────────────────────────────────

(defn- target-countries []
  (let [f (path/join (repo-root) "orgs/kotoba-lang/loop-lei-catalog/resources/target-countries.edn")
        f (if (fs/existsSync f) f (path/join (.cwd js/process) "resources/target-countries.edn"))]
    (set (edn/read-string (fs/readFileSync f "utf8")))))

(defn evaluate
  "Delegates entirely to `catalog-maturity`. No scoring logic here by design."
  [obs targets]
  (cm/plan (assoc obs :target-countries (count targets)) targets))

;; ── decide ──────────────────────────────────────────────────────────────────

(defn decide
  "The top-ranked action the loop can actually execute this cycle.

  An action is only chosen if there is a concrete script that performs it.
  Ranking something the loop cannot do would produce a plan that looks like
  progress and never moves -- so unexecutable actions are reported as
  `:no-runner` and the next executable one is chosen instead."
  [plan]
  (let [runnable #{:acquire-new-country :acquire-outside-dominant-country
                   :discover-contact-routes :archive-missing-documents}
        ranked (:actions plan)
        chosen (first (filter #(and (runnable (:action %))
                                    (not= :unmeasured (:score %)))
                              ranked))]
    {:chosen chosen
     :skipped (vec (remove #(runnable (:action %)) ranked))
     :ranked ranked}))

;; ── act ─────────────────────────────────────────────────────────────────────

(defn- sh [cmd]
  (try
    {:ok true :out (execSync cmd #js {:encoding "utf8" :maxBuffer (* 40 1024 1024)
                                      :stdio "pipe" :cwd (repo-root)})}
    (catch :default e
      {:ok false :out (str (some-> (.-stdout e) str) (some-> (.-stderr e) str))
       :error (.-message e)})))

(defn- summary-line [out]
  (->> (str/split-lines (str out))
       (filter #(str/includes? % "candidates:"))
       last))

(defn act
  "Runs ONE bounded step for the chosen action, then re-projects git into D1.

  Bounded on purpose: a cycle that tried to close the whole gap would run for
  hours, and its evidence entry could not say which change produced which
  movement in the score. One step per cycle keeps each ledger entry a readable
  before/after."
  [{:keys [chosen]} {:keys [dry-run? contact-limit]}]
  (if-not chosen
    {:action :none :note "no runnable action -- every executable axis is already at its ceiling or unmeasured"}
    (let [action (:action chosen)
          cmd (case action
                :discover-contact-routes
                (str "nbb scripts/lei-contact-discover.cljs --concurrency 6"
                     (when contact-limit (str " --limit " contact-limit))
                     (when dry-run? " --dry-run"))
                (:acquire-new-country :acquire-outside-dominant-country :archive-missing-documents)
                (str "nbb scripts/lei-acquire.cljs --concurrency 4" (when dry-run? " --dry-run"))
                nil)]
      (if-not cmd
        {:action action :note ":no-runner"}
        (let [r (sh cmd)
              ;; Re-project even on a partial failure: whatever DID land in git
              ;; belongs in the catalog, and leaving D1 behind would make the
              ;; next cycle observe a state that no longer exists.
              proj (when-not dry-run? (sh "nbb scripts/d1-ingest-cloud-itonami-lei.cljs"))]
          {:action action
           :command cmd
           :ok (:ok r)
           :summary (summary-line (:out r))
           :error (:error r)
           :reprojected (boolean (:ok proj))})))))

;; ── record evidence ─────────────────────────────────────────────────────────

(defn- ledger-path []
  (let [p (path/join (repo-root) "orgs/kotoba-lang/loop-lei-catalog/ledger/lei-catalog-ledger.edn")]
    (if (fs/existsSync (path/dirname p)) p (path/join (.cwd js/process) "ledger/lei-catalog-ledger.edn"))))

(defn record-evidence!
  "Appends ONE line to the append-only ledger. Records the measured before and
  after, not just the after: a score with nothing to compare against cannot
  show whether the cycle helped, and a cycle that made things worse must be as
  visible as one that helped."
  [{:keys [before after action]}]
  (let [p (ledger-path)
        entry {:event/as-of (first (str/split (.toISOString (js/Date.)) #"T"))
               :event/at (.toISOString (js/Date.))
               :event/companies-before (:companies (:observed before))
               :event/companies-after (:companies (:observed after))
               :event/countries-before (count (:country-counts (:observed before)))
               :event/countries-after (count (:country-counts (:observed after)))
               :event/maturity-before (:score (:maturity before))
               :event/maturity-after (:score (:maturity after))
               :event/measured-weight (:measured-weight (:maturity after))
               :event/weakest-axis (:axis (first (:actions after)))
               :event/action (:action action)
               :event/action-ok (:ok action)
               :event/action-summary (:summary action)
               :event/dominant (get-in (:gaps after) [:dominant])
               :event/countries-absent (count (:absent (:gaps after)))}]
    (fs/mkdirSync (path/dirname p) #js {:recursive true})
    (fs/appendFileSync p (str (pr-str entry) "\n"))
    {:ledger-path p :entry entry}))

;; ── cycle ───────────────────────────────────────────────────────────────────

(defn run-cycle!
  "observe -> evaluate -> decide -> act -> record-evidence.

  Observes a SECOND time after acting rather than predicting the outcome:
  what the scripts report and what actually landed in the catalog have already
  diverged once in this project's history (a transact returned ok for rows that
  never became readable), so the after-score is measured, never inferred."
  [{:keys [dry-run? contact-limit] :as opts}]
  (let [targets (target-countries)
        before (evaluate (observe) targets)
        decision (decide before)
        result (act decision opts)
        after (if dry-run? before (evaluate (observe) targets))
        ev (record-evidence! {:before before :after after :action result})]
    {:before before :after after :decision decision :action result
     :ledger-path (:ledger-path ev) :entry (:entry ev)}))
