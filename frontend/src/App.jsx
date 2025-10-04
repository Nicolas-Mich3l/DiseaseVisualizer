import React, { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchCohortFull, groupByAgeAndSex } from "./utils/cohortAPI.js";

const App = () => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loadLoginUI, setIsLogin] = useState(true);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showReset, setShowReset] = useState(false);

  const [selectedDisease, setSelectedDisease] = useState("201820");
  const [selectedMeasurement, setSelectedMeasurement] = useState("3000963");
  const [cohortData, setCohortData] = useState(null);
  const [loading, setLoading] = useState(false);

  const diseases = [
    { id: "443392", name: "Cancer / Non-Cancer" },
    { id: "201820", name: "Diabetes / Non-Diabetes" },
    { id: "316866", name: "Hypertension / Non-Hypertension" },
    { id: "46271022", name: "Chronic Kidney Disease / Non-CKD" },
  ];

  const measurements = [
    { id: "3000963", name: "Hemoglobin [Mass/volume] in Blood" },
    { id: "3004501", name: "Glucose [Mass/volume] in Serum or Plasma" },
    { id: "3012888", name: "Diastolic blood pressure" },
    { id: "3004249", name: "Systolic blood pressure" },
    { id: "2212095", name: "Lipid Panel" },
  ];

  const handleLogin = (e) => {
    e.preventDefault();
    if (email && password) {
      setIsLoggedIn(true);
    }
  };

  const handleReset = (e) => {
    e.preventDefault();
    alert(`Password reset link sent to ${email} -- (this is a mockup)`);
    setShowReset(false);
  };

  const buildCohorts = async () => {
    setLoading(true);
    try {
      const data = await fetchCohortFull(selectedDisease, selectedMeasurement);
      const ageSexData = groupByAgeAndSex().transform(
        data.disease,
        data.healthy,
      );

      setCohortData({
        ageSexData,
        diseaseStats: data.diseaseStats,
        nonDiseaseStats: data.healthyStats,
      });
    } catch (err) {
      alert("Error loading cohort data");
      console.error(err);
    }
    setLoading(false);
  };

  const exportChart = (chartId, format) => {
    alert(`Exporting ${chartId} as ${format.toUpperCase()}`);
  };

  // Auth Screen - Password Reset
  if (!isLoggedIn && showReset) {
    return (
      <div className="auth-container">
        <div className="auth-card">
          <h2 className="section-header">Reset Password</h2>
          <div>
            <div className="mb-4">
              <label className="label">Email</label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="input"
              />
            </div>
            <button onClick={handleReset} className="btn-full mb-2">
              Send Reset Link
            </button>
            <button
              onClick={() => setShowReset(false)}
              className="w-full text-gray-800 font-bold"
            >
              Back to Login
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Auth Screen - Login/Signup
  if (!isLoggedIn) {
    return (
      <div className="auth-container">
        <div className="auth-card">
          <h1 className="text-2xl font-bold mb-6 text-center border-b-4 border-gray-800 pb-2">
            OMOP Visualizer
          </h1>

          <div className="tab-group mb-6">
            <button
              onClick={() => setIsLogin(true)}
              className={loadLoginUI ? "tab-active" : "tab-inactive"}
            >
              Login
            </button>
            <button
              onClick={() => setIsLogin(false)}
              className={!loadLoginUI ? "tab-active" : "tab-inactive"}
            >
              Sign Up
            </button>
          </div>

          <div>
            <div className="mb-4">
              <label className="label">Email</label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="input"
              />
            </div>
            <div className="mb-6">
              <label className="label">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="input"
              />
            </div>
            <button onClick={handleLogin} className="btn-full">
              {loadLoginUI ? "Login" : "Create Account"}
            </button>
          </div>

          {loadLoginUI && (
            <button
              onClick={() => setShowReset(true)}
              className="w-full mt-4 text-gray-800 font-bold underline"
            >
              Forgot password?
            </button>
          )}

          <p className="text-center text-sm text-gray-600 mt-4 italic">
            Demo: This is a mockup, use any values for username & password
          </p>
        </div>
      </div>
    );
  }

  // Main App
  return (
    <div className="min-h-screen bg-gray-200">
      <header className="bg-white border-b-4 border-gray-800">
        <div className="max-w-7xl mx-auto px-4 py-4 flex justify-between items-center">
          <h1 className="text-2xl font-bold">OMOP Visualizer</h1>
          <div className="flex items-center gap-4">
            <span className="text-sm font-bold">{email}</span>
            <button
              onClick={() => setIsLoggedIn(false)}
              className="btn-primary text-sm"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-8">
        {/* Controls */}
        <div className="card mb-6">
          <h2 className="section-header">Build Cohorts</h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div>
              <label className="label">Select Disease</label>
              <select
                value={selectedDisease}
                onChange={(e) => setSelectedDisease(e.target.value)}
                className="input"
              >
                {diseases.map((d) => (
                  <option key={d.id} value={d.id}>{d.name}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="label">Select Measurement</label>
              <select
                value={selectedMeasurement}
                onChange={(e) => setSelectedMeasurement(e.target.value)}
                className="input"
              >
                {measurements.map((m) => (
                  <option key={m.id} value={m.id}>{m.name}</option>
                ))}
              </select>
            </div>

            <div className="flex items-end">
              <button
                onClick={buildCohorts}
                disabled={loading}
                className="btn-full disabled:bg-gray-400"
              >
                {loading ? "Loading..." : "Build Cohorts"}
              </button>
            </div>
          </div>
        </div>

        {cohortData
          ? (
            <>
              {/* Cohort Counts */}
              <div className="grid grid-cols-2 gap-4 mb-6">
                <div className="card">
                  <h3 className="text-lg font-bold mb-2">Disease Cohort</h3>
                  <p className="text-4xl font-bold">
                    {cohortData.diseaseStats.n}
                  </p>
                  <p className="text-sm font-bold text-gray-600">patients</p>
                </div>
                <div className="card">
                  <h3 className="text-lg font-bold mb-2">Non-Disease Cohort</h3>
                  <p className="text-4xl font-bold">
                    {cohortData.nonDiseaseStats.n}
                  </p>
                  <p className="text-sm font-bold text-gray-600">patients</p>
                </div>
              </div>

              {/* Age/Sex Distribution */}
              <div className="card mb-6">
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-bold">
                    Age Group & Sex Distribution
                  </h3>
                  <div className="flex gap-2">
                    <button
                      onClick={() => exportChart("age-sex", "png")}
                      className="btn-primary text-sm"
                    >
                      PNG
                    </button>
                    <button
                      onClick={() => exportChart("age-sex", "pdf")}
                      className="btn-primary text-sm"
                    >
                      PDF
                    </button>
                  </div>
                </div>
                <ResponsiveContainer width="100%" height={400}>
                  <BarChart data={cohortData.ageSexData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="age" />
                    <YAxis
                      label={{
                        value: "Count",
                        angle: -90,
                        position: "insideLeft",
                      }}
                    />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="Disease-Male" fill="#3b82f6" />
                    <Bar dataKey="Disease-Female" fill="#93c5fd" />
                    <Bar dataKey="Non-Disease-Male" fill="#10b981" />
                    <Bar dataKey="Non-Disease-Female" fill="#6ee7b7" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Measurement Comparison */}
              <div className="card mb-6">
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-bold">
                    {cohortData.measurementName} Comparison
                  </h3>
                  <div className="flex gap-2">
                    <button
                      onClick={() => exportChart("boxplot", "png")}
                      className="btn-primary text-sm"
                    >
                      PNG
                    </button>
                    <button
                      onClick={() => exportChart("boxplot", "pdf")}
                      className="btn-primary text-sm"
                    >
                      PDF
                    </button>
                  </div>
                </div>
                <ResponsiveContainer width="100%" height={400}>
                  <BarChart
                    data={[
                      {
                        name: "Disease",
                        value: parseFloat(cohortData.diseaseStats.median),
                      },
                      {
                        name: "Non-Disease",
                        value: parseFloat(cohortData.nonDiseaseStats.median),
                      },
                    ]}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis
                      label={{
                        value: cohortData.measurementUnit,
                        angle: -90,
                        position: "insideLeft",
                      }}
                    />
                    <Tooltip />
                    <Bar dataKey="value" fill="#8884d8" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Summary Statistics */}
              <div className="card">
                <h3 className="section-header">Summary Statistics</h3>
                <div className="grid grid-cols-2 gap-6">
                  <div>
                    <h4 className="font-bold mb-2 text-lg">Disease Cohort</h4>
                    <table className="stats-table">
                      <tbody>
                        <tr>
                          <td>n:</td>
                          <td>{cohortData.diseaseStats.n}</td>
                        </tr>
                        <tr>
                          <td>Median:</td>
                          <td>{cohortData.diseaseStats.median}</td>
                        </tr>
                        <tr>
                          <td>P25:</td>
                          <td>{cohortData.diseaseStats.p25}</td>
                        </tr>
                        <tr>
                          <td>P75:</td>
                          <td>{cohortData.diseaseStats.p75}</td>
                        </tr>
                        <tr>
                          <td>Mean:</td>
                          <td>{cohortData.diseaseStats.mean}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                  <div>
                    <h4 className="font-bold mb-2 text-lg">
                      Non-Disease Cohort
                    </h4>
                    <table className="stats-table">
                      <tbody>
                        <tr>
                          <td>n:</td>
                          <td>{cohortData.nonDiseaseStats.n}</td>
                        </tr>
                        <tr>
                          <td>Median:</td>
                          <td>{cohortData.nonDiseaseStats.median}</td>
                        </tr>
                        <tr>
                          <td>P25:</td>
                          <td>{cohortData.nonDiseaseStats.p25}</td>
                        </tr>
                        <tr>
                          <td>P75:</td>
                          <td>{cohortData.nonDiseaseStats.p75}</td>
                        </tr>
                        <tr>
                          <td>Mean:</td>
                          <td>{cohortData.nonDiseaseStats.mean}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </>
          )
          : (
            <div className="card p-12 text-center">
              <p className="font-bold text-gray-600">
                Select a disease and measurement, then click "Build Cohorts"
              </p>
            </div>
          )}
      </main>
    </div>
  );
};

export default App;
