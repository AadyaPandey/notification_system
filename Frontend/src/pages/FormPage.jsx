import { useState } from "react";
import ResultModal from "../components/ResultModal";
import GrantApplicationForm from "../components/GrantApplicationForm";
import toast from "react-hot-toast";

export default function App() {
  const [result, setResult] = useState(null);
  const [showModal, setShowModal] = useState(false);

  const handleSubmit = async (formData) => {
    const loadingToast = toast.loading("Waking up GrantGuard AI agent...");

    try {
      // Ping backend
      await fetch("https://grand-guard-server.onrender.com");

      toast.success("Agent is online. Starting evaluation...", {
        id: loadingToast,
      });

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

      const data = await response.json();

      setResult(data);
      setShowModal(true);

      toast.success("Evaluation completed.");
    } catch (err) {
      toast.error("Unable to connect to GrantGuard.");
      console.error(err);
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
