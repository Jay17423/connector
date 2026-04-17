(ns connector.middleware
  (:require [ring.util.response :refer [response status]] 
            [taoensso.timbre :as log]))

(defn wrap-error-handler
  "Centralized error handling middleware."
  [handler]
  (fn [req]
    (try
      (handler req)
      (catch AssertionError err
        (let [duration (- (System/currentTimeMillis) (:start-time req))]
          (log/warn {:msg "Invalid request body"
                     :error (.getMessage err)
                     :metric {:duration-ms duration}})
          (status
           (response
            {:status      "error"
             :msg         "Invalid request body"
             :error       (.getMessage err)
             :duration-ms duration})
           400)))

      (catch Exception err
        (let [duration  (- (System/currentTimeMillis) (:start-time req))
              err-data  (ex-data err)
              auth-fail (= 401 (:status err-data))]
          (log/error {:msg    "Dataset load failed"
                      :error  (.getMessage err)
                      :status (:status err-data)
                      :source (:type err-data)
                      :url    (:url err-data)
                      :metric {:duration-ms duration}})
          (status
           (response
            {:status      "error"
             :msg         (if auth-fail
                            "Authentication failed - token expired"
                            "Internal server error")
             :duration-ms duration})
           (if auth-fail 401 500)))))))

(defn wrap-request-timer
  "Attaches start-time to request for duration tracking."
  [handler]
  (fn [req]
    (handler (assoc req :start-time (System/currentTimeMillis)))))