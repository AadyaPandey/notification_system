import { useState } from "react";
function Input({ label, name, value, onChange, type = "text" }) {
  return (
    <div>
      <label className="block mb-2 font-medium text-gray-700">
        {label}
        <span className="text-red-500 ml-1">*</span>
      </label>

      <input
        type={type}
        name={name}
        value={value}
        onChange={onChange}
        required
        className="w-full rounded-lg border p-3 focus:border-blue-500 focus:outline-none"
      />
    </div>
  );
}
export default function GrantApplicationForm({ onSubmit }) {
  const documentsList = [
    "Grant Proposal",
    "Registration Certificate",
    "Budget",
    "Project Timeline",
    "Bank Statement (Last 6 Months)",
    "Bank Details",
    "Beneficiary Information",
    "PAN Card",
    "Audited Financial Statements (Last 3 Years)",
    "Board Resolution",
    "Annual Report 2025",
    "Letters of Partnership",
  ];

  const EMPTY_FORM = {
    organization: "",
    registration_number: "",
    year_established: "",
    years_operating: "",
    contact_person: "",
    designation: "",
    email: "",
    phone: "",
    project_title: "",
    grant_category: "",
    requested_amount: "",
    project_description: "",
    beneficiaries: "",
    timeline: "",
    budget: {
      medical_camps: "",
      medicines_supplies: "",
      training_workshops: "",
      travel_logistics: "",
      administration: "",
      contingency: "",
    },
    documents: [],
    declaration_signed: false,
  };

  const VALID_APPLICATION = {
    organization: "Helping Hands Foundation",
    registration_number: "NGO/2018/45821",
    year_established: 2018,
    years_operating: 8,

    contact_person: "Anita Sharma",
    designation: "Executive Director",
    email: "info@helpinghands.org",
    phone: "+91-9876543210",

    project_title: "Community Health & Nutrition Initiative",
    grant_category: "Healthcare",

    requested_amount: 1400000,

    project_description: `Project Objectives
Improve access to primary healthcare for underserved rural communities through preventive healthcare, maternal health awareness, child nutrition monitoring, and regular medical outreach.

Community Problem
The five target villages have limited access to qualified healthcare professionals, increasing cases of preventable illness and poor maternal and child health outcomes.

Implementation Strategy
The organization will conduct monthly medical camps, distribute essential medicines, organize nutrition awareness sessions, train 40 community health volunteers, and coordinate with local government health workers.

Timeline
The project will run for 12 months with monthly milestones, quarterly reviews, and a final impact.

Beneficiaries
Approximately 2,500 direct beneficiaries including women, children and elderly residents across five villages.

Monitoring and Evaluation
Monthly reports, attendance records, beneficiary surveys, health screening data and quarterly reviews.

Expected Outcomes
• 2,500 beneficiaries receive healthcare services.
• 40 trained community health volunteers.
• Improved preventive healthcare access.

Sustainability
Trained volunteers will continue awareness campaigns with support from the District Health Department.

Partnerships
District Health Department, Primary Health Centres, Village Councils and community NGOs.`,
    beneficiaries: 2500,
    timeline: "12 months",

    budget: {
      medical_camps: 500000,
      medicines_supplies: 280000,
      training_workshops: 180000,
      travel_logistics: 160000,
      administration: 180000,
      contingency: 100000,
    },

    documents: [...documentsList],

    declaration_signed: true,
  };

  const [formData, setFormData] = useState(EMPTY_FORM);
  const [autoFill, setAutoFill] = useState(false);

  const handleChange = (e) => {
    const { name, value, checked, type } = e.target;

    if (type === "checkbox" && name !== "declaration_signed") {
      let docs = [...formData.documents];

      if (checked) docs.push(value);
      else docs = docs.filter((d) => d !== value);

      setFormData({
        ...formData,
        documents: docs,
      });

      return;
    }

    if (name === "declaration_signed") {
      setFormData({
        ...formData,
        declaration_signed: checked,
      });

      return;
    }

    setFormData({
      ...formData,
      [name]: value,
    });
  };
  const handleAutoFill = (e) => {
    const checked = e.target.checked;

    setAutoFill(checked);

    if (checked) {
      // clone so React gets a fresh object
      setFormData(JSON.parse(JSON.stringify(VALID_APPLICATION)));
    } else {
      setFormData(JSON.parse(JSON.stringify(EMPTY_FORM)));
    }
  };
  const handleBudget = (e) => {
    setFormData({
      ...formData,
      budget: {
        ...formData.budget,
        [e.target.name]: e.target.value,
      },
    });
  };

  const submit = async (e) => {
    e.preventDefault();

    await onSubmit(formData);

    setFormData(JSON.parse(JSON.stringify(EMPTY_FORM)));
    setAutoFill(false);
  };

  return (
    <div className="min-h-screen bg-pink-100 py-10 px-5">
      <div className="max-w-5xl mx-auto bg-pink-50 shadow-xl rounded-xl p-10">
        <h1 className="text-4xl font-bold text-center mb-10">
          NGO Grant Application
        </h1>

        <form onSubmit={submit} className="space-y-10">
          {/* Organization */}

          <section>
            <h2 className="text-2xl font-semibold mb-6 border-b pb-2">
              Organization Details
            </h2>

            <div className="grid md:grid-cols-2 gap-6">
              <Input
                label="Organization Name"
                name="organization"
                value={formData.organization}
                onChange={handleChange}
              />

              <Input
                label="Registration Number"
                name="registration_number"
                value={formData.registration_number}
                onChange={handleChange}
              />

              <Input
                label="Year Established"
                name="year_established"
                type="number"
                value={formData.year_established}
                onChange={handleChange}
              />

              <Input
                label="Years Operating"
                name="years_operating"
                type="number"
                value={formData.years_operating}
                onChange={handleChange}
              />
            </div>
          </section>

          {/* Contact */}

          <section>
            <h2 className="text-2xl font-semibold mb-6 border-b pb-2">
              Contact Information
            </h2>

            <div className="grid md:grid-cols-2 gap-6">
              <Input
                label="Contact Person"
                name="contact_person"
                value={formData.contact_person}
                onChange={handleChange}
              />

              <Input
                label="Designation"
                name="designation"
                value={formData.designation}
                onChange={handleChange}
              />

              <Input
                label="Email"
                name="email"
                type="email"
                value={formData.email}
                onChange={handleChange}
              />

              <Input
                label="Phone"
                name="phone"
                value={formData.phone}
                onChange={handleChange}
              />
            </div>
          </section>

          {/* Project */}

          <section>
            <h2 className="text-2xl font-semibold mb-6 border-b pb-2">
              Project Details
            </h2>

            <div className="grid md:grid-cols-2 gap-6">
              <Input
                label="Project Title"
                name="project_title"
                value={formData.project_title}
                onChange={handleChange}
              />

              <Input
                label="Grant Category"
                name="grant_category"
                value={formData.grant_category}
                onChange={handleChange}
              />

              <Input
                label="Requested Amount"
                name="requested_amount"
                type="number"
                value={formData.requested_amount}
                onChange={handleChange}
              />

              <Input
                label="Beneficiaries"
                name="beneficiaries"
                type="number"
                value={formData.beneficiaries}
                onChange={handleChange}
              />

              <Input
                label="Timeline"
                name="timeline"
                value={formData.timeline}
                onChange={handleChange}
              />
            </div>

            <div className="mt-6">
              <label className="block mb-2 font-medium">
                Project Description
                <span className="text-red-500 ml-1">*</span>
              </label>

              <textarea
                required
                rows={10}
                name="project_description"
                value={formData.project_description}
                onChange={handleChange}
                className="w-full border rounded-lg p-4 focus:border-blue-500 focus:outline-none"
              />
            </div>
          </section>

          {/* Budget */}

          <section>
            <h2 className="text-2xl font-semibold mb-6 border-b pb-2">
              Budget Breakdown
            </h2>

            <div className="grid md:grid-cols-2 gap-6">
              {Object.keys(formData.budget).map((item) => (
                <div key={item}>
                  <label className="block mb-2 capitalize font-medium">
                    {item.replaceAll("_", " ")}
                  </label>

                  <input
                    type="number"
                    name={item}
                    value={formData.budget[item]}
                    onChange={handleBudget}
                    className="w-full border rounded-lg p-3 focus:border-blue-500 focus:outline-none"
                  />
                </div>
              ))}
            </div>
          </section>

          {/* Documents */}

          <section>
            <h2 className="text-2xl font-semibold mb-6 border-b pb-2">
              Documents Submitted
            </h2>

            <div className="grid md:grid-cols-2 gap-3">
              {documentsList.map((doc) => (
                <label key={doc} className="flex items-center gap-3">
                  <input
                    type="checkbox"
                    value={doc}
                    name="documents"
                    checked={formData.documents.includes(doc)}
                    onChange={handleChange}
                  />

                  {doc}
                </label>
              ))}
            </div>
          </section>

          {/* Declaration */}

          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              name="declaration_signed"
              checked={formData.declaration_signed}
              onChange={handleChange}
              required
            />

            <label>I declare that the information provided is accurate.</label>
          </div>
          <div className="flex items-center gap-3 rounded-lg bg-blue-50 p-4 border border-blue-200">
            <input
              type="checkbox"
              checked={autoFill}
              onChange={handleAutoFill}
              className="h-5 w-5"
            />

            <label className="font-medium text-blue-900 cursor-pointer">
              Auto-fill the form using the sample NGO application
            </label>
          </div>
          <button
            type="submit"
            className="w-full bg-black hover:bg-black-700 text-white font-semibold py-4 rounded-lg transition"
          >
            Submit Application
          </button>
        </form>
      </div>
    </div>
  );
}
