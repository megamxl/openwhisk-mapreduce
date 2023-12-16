package ow

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type Params struct {
	Value        json.RawMessage `json:"value"`
	ActivationID string          `json:"activation_id"`
	ActionName   string          `json:"action_name"`
	Deadline     string          `json:"deadline"`
	ApiKey       string          `json:"api_key"`
	Namespace    string          `json:"namespace"`
}

type ErrResponse struct {
	Error string `json:"error"`
}

type Callback func(json.RawMessage) (interface{}, error)

var action Callback

func RegisterAction(cb Callback) {
	action = cb
	setupHandlers()
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
}

func sendError(w http.ResponseWriter, code int, cause string) {
	fmt.Println("action error:", cause)
	errResponse := ErrResponse{Error: cause}
	b, err := json.Marshal(errResponse)
	if err != nil {
		fmt.Println("error marshalling error response:", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

func runHandler(w http.ResponseWriter, r *http.Request) {

	params := Params{}

	defer r.Body.Close()
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error reading request body: %v", err))
		return
	}

	if err := json.Unmarshal(bodyBytes, &params); err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error unmarshaling request: %v", err))
		return
	}

	// Propagate values from params such as api_key into environment variables
	if err := propagateParamsToEnvironment(params); err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error setting environment for action: %v", err))
		return
	}

	response, err := action(params.Value)

	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error executing action: %v", err))
		return
	}

	b, err := json.Marshal(response)
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error marshaling response: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Set the content-length to avoid using chunked transfer encoding, which will cause OpenWhisk to return error:
	//   "error": "The action did not produce a valid response and exited unexpectedly."
	// Workaround source: https://stackoverflow.com/questions/34794647/disable-chunked-transfer-encoding-in-go-without-using-content-length
	// Related issue: https://github.com/jthomas/ow/issues/2
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(b)))

	numBytesWritten, err := w.Write(b)
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error writing response: %v", err))
		return
	}

	if numBytesWritten != len(b) {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Only wrote %d of %d bytes to response", numBytesWritten, len(b)))
		return
	}

}

func propagateParamsToEnvironment(params Params) error {

	if params.ApiKey != "" {
		if err := os.Setenv("__OW_API_KEY", params.ApiKey); err != nil {
			return err
		}
	}

	if params.Namespace != "" {
		if err := os.Setenv("__OW_NAMESPACE", params.Namespace); err != nil {
			return err
		}
	}

	if params.ActionName != "" {
		if err := os.Setenv("__OW_ACTION_NAME", params.ActionName); err != nil {
			return err
		}
	}

	if params.ActivationID != "" {
		if err := os.Setenv("__OW_ACTIVATION_ID", params.ActivationID); err != nil {
			return err
		}
	}

	if params.Deadline != "" {
		if err := os.Setenv("__OW_DEADLINE", params.Deadline); err != nil {
			return err
		}
	}

	return nil
}

func setupHandlers() {
	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/run", runHandler)
	http.ListenAndServe(":8080", nil)
}
