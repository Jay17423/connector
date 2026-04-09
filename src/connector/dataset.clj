(ns connector.dataset
  "Loads datasets into Spark using production-safe CSV options."
  (:require [connector.cloud-fetch :as fetcher]
            [taoensso.timbre :as log]
            [clojure.string :as str]))

(defn- json-escape
  "Escapes special characters for safe JSON string values."
  [value]
  (-> (str value)
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn configure-s3!
  "Applies S3 credentials and settings to Spark Hadoop configuration."
  [spark {:keys [access-key secret-key region version-id]}]
  (let [hconf (-> spark
                  .sparkContext
                  .hadoopConfiguration)]
    (when access-key
      (.set hconf "fs.s3a.access.key" access-key))
    (when secret-key
      (.set hconf "fs.s3a.secret.key" secret-key))
    (when region
      (.set hconf "fs.s3a.endpoint" (str "s3." region ".amazonaws.com")))
    (when version-id
      (.set hconf "fs.s3a.version.id" version-id))))

(defn configure-gcs!
  "Configures GCS authentication and filesystem settings in Spark."
  [spark {:keys [project-id client-email private-key]}]
  (let [hconf (-> spark
                  .sparkContext
                  .hadoopConfiguration)]
    (when project-id
      (.set hconf "fs.gs.project.id" project-id))

    (when (and project-id client-email private-key)
      (let [json-key
            (str "{"
                 "\"type\":\"service_account\","
                 "\"project_id\":\"" (json-escape project-id) "\","
                 "\"private_key_id\":\"placeholder\","
                 "\"private_key\":\"" (json-escape private-key) "\","
                 "\"client_email\":\"" (json-escape client-email) "\","
                 "\"client_id\":\"placeholder\","
                 "\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\","
                 "\"token_uri\":\"https://oauth2.googleapis.com/token\""
                 "}")
            tmp-file (java.io.File/createTempFile "gcs-key-" ".json")]
        (.deleteOnExit tmp-file)
        (spit (.getAbsolutePath tmp-file) json-key)
        (.set hconf "fs.gs.auth.type" "SERVICE_ACCOUNT_JSON_KEYFILE")
        (.set hconf "fs.gs.auth.service.account.json.keyfile"
              (.getAbsolutePath tmp-file))))))

(defn resolve-source
  "Resolves dataset source path. Returns just the path string."
  [config]
  (let [{:keys [type path cred]} config]
    (cond
      (= type :local)
      path

      (= type :s3)
      (str "s3a://" (:bucket cred) "/" (:key cred))

      (= type :gcs)
      (str "gs://" (:bucket cred) "/"
           (:key cred)
           (when-let [g (:generation cred)]
             (str "#" g)))

      (contains? #{:gdrive :dropbox} type)
      (fetcher/fetch-file! type cred)

      :else
      (throw
       (ex-info "Unsupported dataset source"
                {:type type})))))

(defn read-dataset
  "Loads dataset into Spark using standardized CSV options."
  [spark {:keys [options cred] :as config}]

  (when (= :s3 (:type config))
    (configure-s3! spark cred))

  (when (= :gcs (:type config))
    (configure-gcs! spark cred))

  (let [opts (merge {:header true
                     :delimiter ","}
                    options)
        path (resolve-source config)]

    (log/info {:msg "Reading dataset into Spark"
               :metric {:type (:type config)
                        :path path}})
    (try
      (-> spark
          .read
          (.format "csv")
          (.option "header" (if (:header opts) "true" "false"))
          (.option "delimiter" (:delimiter opts))
          (.option "mode" "PERMISSIVE")
          (.option "nullValue" "")
          (.option "ignoreLeadingWhiteSpace" "true")
          (.option "ignoreTrailingWhiteSpace" "true")
          (.option "columnNameOfCorruptRecord" "_corrupt_record")
          (.load path))
      (catch Exception err
        (throw (ex-info "Failed to read dataset"
                        {:type  (:type config)
                         :path  path
                         :error (.getMessage err)}
                        err))))))