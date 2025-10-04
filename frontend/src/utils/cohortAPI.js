const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:5000";

/**
 * Fetch cohort data from backend
 * @param {string} diseaseId - Disease concept ID
 * @returns {Promise<{disease: Array, healthy: Array}>}
 */
export async function fetchCohortData(diseaseId) {
  const response = await fetch(
    `${API_BASE_URL}/api/cohort?disease_id=${diseaseId}`,
  );

  if (!response.ok) {
    throw new Error(`API error: ${response.statusText}`);
  }

  return await response.json();
}

/**
 * Fetch cohort data with measurements and stats
 * @param {string} diseaseId - Disease concept ID
 * @param {string} measurementId - Measurement concept ID
 * @returns {Promise<{disease: Array, healthy: Array, diseaseStats: Object, healthyStats: Object}>}
 */
export async function fetchCohortFull(diseaseId, measurementId) {
  const url = measurementId
    ? `${API_BASE_URL}/api/cohort-full?disease_id=${diseaseId}&measurement_id=${measurementId}`
    : `${API_BASE_URL}/api/cohort-full?disease_id=${diseaseId}`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`API error: ${response.statusText}`);
  }

  return await response.json();
}

/**
 * Fetch summary statistics for measurements
 * @param {string} diseaseId - Disease concept ID
 * @param {string} measurementId - Measurement concept ID
 * @returns {Promise<{diseaseStats: Object, healthyStats: Object}>}
 */
export async function fetchStats(diseaseId, measurementId) {
  const response = await fetch(
    `${API_BASE_URL}/api/stats?disease_id=${diseaseId}&measurement_id=${measurementId}`,
  );

  if (!response.ok) {
    throw new Error(`API error: ${response.statusText}`);
  }

  return await response.json();
}

/**
 * Calculate statistics from array of values (client-side fallback)
 * @param {Array<number>} values - Array of numeric values
 * @returns {Object} Statistics object {n, median, p25, p75, mean}
 */
export function getStats(values) {
  // Filter out null/undefined/NaN values
  const cleanValues = values.filter((v) =>
    v !== null && v !== undefined && !isNaN(v)
  );

  if (cleanValues.length === 0) {
    return {
      n: 0,
      median: null,
      p25: null,
      p75: null,
      mean: null,
    };
  }

  const sorted = [...cleanValues].sort((a, b) => a - b);
  const n = sorted.length;

  const percentile = (arr, p) => {
    const index = (p / 100) * (arr.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const weight = index % 1;

    if (lower === upper) return arr[lower];
    return arr[lower] * (1 - weight) + arr[upper] * weight;
  };

  return {
    n,
    median: percentile(sorted, 50).toFixed(2),
    p25: percentile(sorted, 25).toFixed(2),
    p75: percentile(sorted, 75).toFixed(2),
    mean: (cleanValues.reduce((a, b) => a + b, 0) / n).toFixed(2),
  };
}

/**
 * Group cohort data by age group and sex
 * @returns {Object} Transformer object with transform method
 */
export function groupByAgeAndSex() {
  return {
    /**
     * Transform disease and healthy cohorts into age/sex distribution data
     * @param {Array} disease - Disease cohort data
     * @param {Array} healthy - Healthy cohort data
     * @returns {Array} Age/sex distribution suitable for recharts
     */
    transform(disease, healthy) {
      const ageGroups = ["<20", "20-40", "40-60", "60+"];

      const countByAgeGenderCohort = (data, cohortType) => {
        const counts = {};

        data.forEach((person) => {
          const age = person.age_group || "Unknown";
          const gender = person.gender || "Unknown";
          const key = `${age}-${gender}-${cohortType}`;
          counts[key] = (counts[key] || 0) + 1;
        });

        return counts;
      };

      const diseaseCounts = countByAgeGenderCohort(disease, "Disease");
      const healthyCounts = countByAgeGenderCohort(healthy, "Non-Disease");

      return ageGroups.map((age) => ({
        age,
        "Disease-Male": diseaseCounts[`${age}-Male-Disease`] || 0,
        "Disease-Female": diseaseCounts[`${age}-Female-Disease`] || 0,
        "Non-Disease-Male": healthyCounts[`${age}-Male-Non-Disease`] || 0,
        "Non-Disease-Female": healthyCounts[`${age}-Female-Non-Disease`] || 0,
      }));
    },
  };
}
