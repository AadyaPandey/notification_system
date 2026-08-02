export default function ResultModal({ open, result, onClose }) {
  if (!open || !result) return null;

  const decision = result.final_decision;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-2xl rounded-2xl bg-white p-8 shadow-2xl">
        <div className="flex items-center justify-between">
          <h2 className="text-3xl font-bold">Grant Review Result</h2>

          <button onClick={onClose} className="text-3xl hover:text-red-500">
            ×
          </button>
        </div>

        <div className="mt-8">
          <p className="font-semibold mb-2">Decision</p>

          <span
            className={`rounded-lg px-5 py-2 text-lg font-bold text-white ${
              decision.decision === "APPROVE" ? "bg-green-600" : "bg-red-600"
            }`}
          >
            {decision.decision}
          </span>

          <div className="mt-6">
            <h3 className="font-semibold">Confidence</h3>
            <p>{decision.confidence}%</p>
          </div>

          <div className="mt-6">
            <h3 className="font-semibold">Summary</h3>
            <p>{decision.summary}</p>
          </div>

          <div className="mt-6">
            <h3 className="font-semibold">Reasons</h3>

            <ul className="list-disc pl-5">
              {decision.reasons.map((reason, i) => (
                <li key={i}>{reason}</li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
