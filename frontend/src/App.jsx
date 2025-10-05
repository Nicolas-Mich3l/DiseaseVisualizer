import React, { useState } from "react";
import Plot from "react-plotly.js";
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

      // Get measurement values for box plot
      const diseaseValues = data.diseaseMeasurments
        .map((d) => Math.floor(d))
        .filter((v) => v !== null);

      const healthyValues = data.healthyMeasurements
        .map((d) => Math.floor(d))
        .filter((v) => v !== null);

      const ageSexData = groupByAgeAndSex().transform(
        data.disease,
        data.healthy,
      );

      const selectedMeasurementInfo = measurements.find((m) =>
        m.id === selectedMeasurement
      );

      setCohortData({
        ageSexData,
        diseaseStats: data.diseaseStats,
        nonDiseaseStats: data.healthyStats,
        diseaseValues,
        healthyValues,
        diseaseCount: data.disease.length,
        healthyCount: data.healthy.length,
        measurementName: selectedMeasurementInfo?.name || "Measurement",
        measurementUnit: selectedMeasurementInfo?.unit || "units",
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
                    {cohortData.diseaseCount}
                  </p>
                  <p className="text-sm font-bold text-gray-600">patients</p>
                </div>
                <div className="card">
                  <h3 className="text-lg font-bold mb-2">Non-Disease Cohort</h3>
                  <p className="text-4xl font-bold">
                    {cohortData.healthyCount}
                  </p>
                  <p className="text-sm font-bold text-gray-600">patients</p>
                </div>
              </div>

              {/* Age/Sex Distribution */}
              <div className="card mb-6">
                <h3 className="text-lg font-bold mb-4">
                  Age Group & Sex Distribution
                </h3>
                <Plot
                  data={[
                    {
                      x: cohortData.ageSexData.map((d) => d.age),
                      y: cohortData.ageSexData.map((d) => d["Disease-Male"]),
                      name: "Disease-Male",
                      type: "bar",
                      marker: { color: "#3b82f6" },
                    },
                    {
                      x: cohortData.ageSexData.map((d) => d.age),
                      y: cohortData.ageSexData.map((d) => d["Disease-Female"]),
                      name: "Disease-Female",
                      type: "bar",
                      marker: { color: "#93c5fd" },
                    },
                    {
                      x: cohortData.ageSexData.map((d) => d.age),
                      y: cohortData.ageSexData.map((d) =>
                        d["Non-Disease-Male"]
                      ),
                      name: "Non-Disease-Male",
                      type: "bar",
                      marker: { color: "#10b981" },
                    },
                    {
                      x: cohortData.ageSexData.map((d) => d.age),
                      y: cohortData.ageSexData.map((d) =>
                        d["Non-Disease-Female"]
                      ),
                      name: "Non-Disease-Female",
                      type: "bar",
                      marker: { color: "#6ee7b7" },
                    },
                  ]}
                  layout={{
                    barmode: "group",
                    xaxis: { title: "Age Group" },
                    yaxis: { title: "Count" },
                    height: 400,
                    margin: { t: 20, b: 50, l: 60, r: 20 },
                    font: { family: "Arial, sans-serif", size: 12 },
                  }}
                  config={{
                    displayModeBar: true,
                    displaylogo: false,
                    modeBarButtonsToRemove: ["pan2d", "lasso2d", "select2d"],
                    toImageButtonOptions: {
                      format: "png",
                      filename: "age-sex-distribution",
                      height: 600,
                      width: 1000,
                      scale: 2,
                    },
                  }}
                  style={{ width: "100%" }}
                />
              </div>

              {/* Box Plot Comparison */}
              <div className="card mb-6">
                <h3 className="text-lg font-bold mb-4">
                  {cohortData.measurementName} Comparison (Box Plot)
                </h3>
                {cohortData.diseaseValues.length > 0 ||
                    cohortData.healthyValues.length > 0
                  ? (
                    <Plot
                      data={[
                        {
                          y: cohortData.diseaseValues,
                          name: "Disease",
                          type: "box",
                          marker: { color: "#ef4444" },
                          boxmean: "sd",
                        },
                        {
                          y: cohortData.healthyValues,
                          name: "Non-Disease",
                          type: "box",
                          marker: { color: "#3b82f6" },
                          boxmean: "sd",
                        },
                      ]}
                      layout={{
                        yaxis: { title: cohortData.measurementUnit },
                        height: 400,
                        margin: { t: 20, b: 50, l: 60, r: 20 },
                        font: { family: "Arial, sans-serif", size: 12 },
                        showlegend: true,
                      }}
                      config={{
                        displayModeBar: true,
                        displaylogo: false,
                        modeBarButtonsToRemove: [
                          "pan2d",
                          "lasso2d",
                          "select2d",
                        ],
                        toImageButtonOptions: {
                          format: "png",
                          filename: "measurement-boxplot",
                          height: 600,
                          width: 1000,
                          scale: 2,
                        },
                      }}
                      style={{ width: "100%" }}
                    />
                  )
                  : (
                    <div className="flex items-center justify-center h-96">
                      <div className="text-center">
                        <p className="text-xl font-bold text-gray-600 mb-2">
                          No Measurement Data Available
                        </p>
                        <p className="text-sm text-gray-500">
                          No measurements found for the selected cohorts and
                          measurement type. The value_as_number field of AWS
                          OMOP data is empty. sdf lipid measurment is generated
                          by a script as runtime to demo visualizations.
                        </p>
                      </div>
                    </div>
                  )}
              </div>

              {/* Summary Statistics */}
              {(cohortData.diseaseStats.n > 0 ||
                cohortData.nonDiseaseStats.n > 0) && (
                <div className="card">
                  <h3 className="section-header">
                    Measurment Statistics Summary
                  </h3>
                  <div className="grid grid-cols-2 gap-6">
                    {cohortData.diseaseStats.n > 0
                      ? (
                        <div>
                          <h4 className="font-bold mb-2 text-lg">
                            Disease Cohort
                          </h4>
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
                      )
                      : (
                        <div className="flex items-center justify-center">
                          <p className="text-gray-500 italic">
                            No measurement data available
                          </p>
                        </div>
                      )}

                    {cohortData.nonDiseaseStats.n > 0
                      ? (
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
                      )
                      : (
                        <div className="flex items-center justify-center">
                          <p className="text-gray-500 italic">
                            No measurement data available
                          </p>
                        </div>
                      )}
                  </div>
                </div>
              )}
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
