import { useState } from "react";
import ResultModal from "../components/ResultModal";
import GrantApplicationForm from "../components/GrantApplicationForm";
import toast from "react-hot-toast";

export default function App() {
  const [result, setResult] = useState(null);
  const [showModal, setShowModal] = useState(false);

  const handleSubmit = async (formData) => {
    console.log("========== HANDLE SUBMIT START ==========");
    console.log("Form Data:", formData);

    const loadingToast = toast.loading("Waking up GrantGuard AI agent...");

    try {
      console.log("1. Pinging backend...");

      const pingResponse = await fetch(
        "https://grand-guard-server.onrender.com",
      );

      console.log("Ping Status:", pingResponse.status);

      toast.success("Agent is online. Starting evaluation...", {
        id: loadingToast,
      });

      console.log("2. Sending evaluation request...");

      const response = await fetch(
        "https://grand-guard-server.onrender.com/evaluate",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(formData),
        },
      );

      console.log("Evaluation Response Status:", response.status);

      const data = await response.json();

      console.log("Evaluation Response:");
      console.log(data);

      setResult(data);
      setShowModal(true);

      toast.success("Evaluation completed.");

      console.log("3. Preparing notification...");

      const token = localStorage.getItem("token");

      console.log("JWT Token:", token);

      if (!token) {
        console.error("No JWT token found in localStorage!");
        toast.error("User not authenticated.");
        return;
      }

      const decision = data.final_decision;

      console.log("Decision Object:");
      console.log(decision);

      const message = `
Decision: ${decision.decision}
Confidence: ${decision.confidence}%

Summary:
${decision.summary}

Reasons:
${decision.reasons.map((r) => `• ${r}`).join("\n")}
`;

      console.log("Notification Message:");
      console.log(message);

      toast.loading("Sending Notification...");

      console.log("4. Sending notification request...");

      const notificationResponse = await fetch(
        "http://localhost:8000/notifications",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            subject: "Fund-Wise Review Result",
            message,
          }),
        },
      );

      console.log("Notification Response Status:", notificationResponse.status);

      const notificationText = await notificationResponse.text();

      console.log("Notification Response Body:");
      console.log(notificationText);

      if (!notificationResponse.ok) {
        throw new Error(
          `Notification API failed: ${notificationResponse.status}\n${notificationText}`,
        );
      }

      toast.success("Notification sent successfully!");

      console.log("========== HANDLE SUBMIT SUCCESS ==========");
    } catch (err) {
      console.error("========== ERROR ==========");
      console.error(err);

      toast.error("Unable to connect to Fund-Wise.");
    }
  };

  return (
    <>
      <GrantApplicationForm onSubmit={handleSubmit} />

      <ResultModal
        open={showModal}
        result={result}
        onClose={() => setShowModal(false)}
      />
    </>
  );
}
