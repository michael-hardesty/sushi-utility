import React, { useState, useRef } from 'react';
import { Download, Upload, Play, AlertCircle, CheckCircle, Clock, FileText } from 'lucide-react';
import './App.css';

function App() {
  const [customerIds, setCustomerIds] = useState('');
  const [beginDate, setBeginDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [requestorId, setRequestorId] = useState('');
  const [reportType, setReportType] = useState('tr_j3');
  const [processing, setProcessing] = useState(false);
  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [results, setResults] = useState([]);
  const [errors, setErrors] = useState([]);
  const [aggregatedData, setAggregatedData] = useState([]);
  const fileInputRef = useRef(null);

  // Your Supabase project URL - update this after setup
  const SUPABASE_FUNCTION_URL = 'https://YOUR_PROJECT_REF.supabase.co/functions/v1/sushi-proxy';

  const reportTypes = [
    { value: 'tr_j1', label: 'TR_J1 - Journal Requests' },
    { value: 'tr_j2', label: 'TR_J2 - Journal Access Denied' },
    { value: 'tr_j3', label: 'TR_J3 - Journal Usage by Access Type' },
    { value: 'tr_j4', label: 'TR_J4 - Journal Requests by YOP' }
  ];

  const handleFileUpload = (event) => {
    const file = event.target.files[0];
    if (file && file.type === 'text/csv') {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split('\n');
        const header = lines[0].toLowerCase();
        
        if (header.includes('customer_id')) {
          // Parse CSV and extract customer IDs
          const ids = lines.slice(1)
            .map(line => line.split(',')[0])
            .filter(id => id && id.trim())
            .join('\n');
          setCustomerIds(ids);
        } else {
          // Assume it's a simple list of IDs
          setCustomerIds(text);
        }
      };
      reader.readAsText(file);
    }
  };

  const queryApi = async (customerId) => {
    const params = new URLSearchParams({
      requestor_id: requestorId,
      customer_id: customerId,
      begin_date: beginDate,
      end_date: endDate
    });
    
    const url = `${SUPABASE_FUNCTION_URL}/${reportType}?${params}`;
    
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        }
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
      }
      
      return await response.json();
    } catch (error) {
      if (error.message.includes('Failed to fetch')) {
        throw new Error('Cannot connect to server. Please check your configuration.');
      }
      throw error;
    }
  };

  const processJsonData = (jsonData, customerId) => {
    const processedData = [];
    
    if (!jsonData || !jsonData.Report_Items) {
      return processedData;
    }

    const institutionName = jsonData.Report_Header?.Institution_Name || '';

    jsonData.Report_Items.forEach(item => {
      const platform = item.Platform || '';
      const title = item.Title || '';
      const accessType = item.Access_Type || '';
      const publisher = item.Publisher || '';

      (item.Performance || []).forEach(performance => {
        const period = performance.Period || {};
        const periodBeginDate = period.Begin_Date || '';
        const periodEndDate = period.End_Date || '';

        (performance.Instance || []).forEach(instance => {
          const metricType = instance.Metric_Type || '';
          const count = instance.Count || 0;

          processedData.push({
            'Customer ID': customerId,
            'Institution Name': institutionName,
            'Platform': platform,
            'Title': title,
            'Access Type': accessType,
            'Publisher': publisher,
            'Begin Date': periodBeginDate,
            'End Date': periodEndDate,
            'Metric Type': metricType,
            'Total': count
          });
        });
      });
    });

    return processedData;
  };

  const handleProcess = async () => {
    const ids = customerIds.split('\n').filter(id => id.trim());
    
    if (!ids.length || !beginDate || !endDate || !requestorId) {
      alert('Please fill in all required fields and provide at least one customer ID.');
      return;
    }

    setProcessing(true);
    setProgress({ current: 0, total: ids.length });
    setResults([]);
    setErrors([]);
    setAggregatedData([]);

    const allData = [];
    const processResults = [];
    const processErrors = [];

    for (let i = 0; i < ids.length; i++) {
      const customerId = ids[i].trim();
      setProgress({ current: i + 1, total: ids.length });

      try {
        const jsonData = await queryApi(customerId);
        const processedData = processJsonData(jsonData, customerId);
        allData.push(...processedData);
        
        processResults.push({
          customerId,
          status: 'success',
          recordCount: processedData.length
        });
      } catch (error) {
        processErrors.push({
          customerId,
          error: error.message
        });
      }

      // Small delay to prevent overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    setResults(processResults);
    setErrors(processErrors);
    setAggregatedData(allData);
    setProcessing(false);
  };

  const downloadCsv = () => {
    if (!aggregatedData.length) return;

    const headers = Object.keys(aggregatedData[0]);
    const csvContent = [
      headers.join(','),
      ...aggregatedData.map(row => 
        headers.map(header => {
          const value = row[header];
          return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
        }).join(',')
      )
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sushi_report_${reportType}_${beginDate}_${endDate}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="max-w-6xl mx-auto p-6 bg-white">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">SUSHI Bulk Data Utility</h1>
        <p className="text-gray-600">Pull usage data from hosting platform API in bulk for multiple subscribers</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Input Panel */}
        <div className="space-y-6">
          <div className="bg-gray-50 p-6 rounded-lg border">
            <h2 className="text-xl font-semibold mb-4 flex items-center">
              <FileText className="mr-2" size={20} />
              Configuration
            </h2>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Report Type *
                </label>
                <select
                  value={reportType}
                  onChange={(e) => setReportType(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {reportTypes.map(type => (
                    <option key={type.value} value={type.value}>
                      {type.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Begin Date *
                  </label>
                  <input
                    type="date"
                    value={beginDate}
                    onChange={(e) => setBeginDate(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    End Date *
                  </label>
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Requestor ID *
                </label>
                <input
                  type="text"
                  value={requestorId}
                  onChange={(e) => setRequestorId(e.target.value)}
                  placeholder="Enter requestor ID"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
          </div>

          <div className="bg-gray-50 p-6 rounded-lg border">
            <h3 className="text-lg font-semibold mb-4 flex items-center">
              <Upload className="mr-2" size={18} />
              Customer IDs
            </h3>
            
            <div className="space-y-4">
              <div>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="w-full px-4 py-2 border border-dashed border-gray-300 rounded-md hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
                >
                  Upload CSV File
                </button>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleFileUpload}
                  className="hidden"
                />
              </div>
              
              <div className="text-center text-gray-500">or</div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Enter Customer IDs (one per line) *
                </label>
                <textarea
                  value={customerIds}
                  onChange={(e) => setCustomerIds(e.target.value)}
                  placeholder="12345&#10;67890&#10;11111"
                  rows="8"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <p className="text-sm text-gray-500 mt-2">
                  {customerIds.split('\n').filter(id => id.trim()).length} customer IDs entered
                </p>
              </div>
            </div>

            <button
              onClick={handleProcess}
              disabled={processing}
              className="w-full mt-6 px-6 py-3 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
            >
              {processing ? (
                <>
                  <Clock className="mr-2 animate-spin" size={20} />
                  Processing... ({progress.current}/{progress.total})
                </>
              ) : (
                <>
                  <Play className="mr-2" size={20} />
                  Generate Reports
                </>
              )}
            </button>
          </div>
        </div>

        {/* Results Panel */}
        <div className="space-y-6">
          {processing && (
            <div className="bg-blue-50 p-6 rounded-lg border border-blue-200">
              <h3 className="text-lg font-semibold mb-4 text-blue-800">Processing Status</h3>
              <div className="w-full bg-blue-200 rounded-full h-3 mb-4">
                <div
                  className="bg-blue-600 h-3 rounded-full transition-all duration-300"
                  style={{ width: `${(progress.current / progress.total) * 100}%` }}
                ></div>
              </div>
              <p className="text-blue-700">
                Processing {progress.current} of {progress.total} customer IDs...
              </p>
            </div>
          )}

          {results.length > 0 && (
            <div className="bg-green-50 p-6 rounded-lg border border-green-200">
              <h3 className="text-lg font-semibold mb-4 text-green-800 flex items-center">
                <CheckCircle className="mr-2" size={20} />
                Processing Complete
              </h3>
              
              <div className="space-y-2 mb-4">
                <p className="text-green-700">
                  Successfully processed: {results.length} customer IDs
                </p>
                <p className="text-green-700">
                  Total records retrieved: {aggregatedData.length}
                </p>
                {errors.length > 0 && (
                  <p className="text-red-600">
                    Failed: {errors.length} customer IDs
                  </p>
                )}
              </div>

              {aggregatedData.length > 0 && (
                <button
                  onClick={downloadCsv}
                  className="w-full px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 flex items-center justify-center"
                >
                  <Download className="mr-2" size={20} />
                  Download Aggregated CSV ({aggregatedData.length} rows)
                </button>
              )}
            </div>
          )}

          {errors.length > 0 && (
            <div className="bg-red-50 p-6 rounded-lg border border-red-200">
              <h3 className="text-lg font-semibold mb-4 text-red-800 flex items-center">
                <AlertCircle className="mr-2" size={20} />
                Errors ({errors.length})
              </h3>
              <div className="max-h-40 overflow-y-auto space-y-2">
                {errors.map((error, index) => (
                  <div key={index} className="text-sm text-red-700 bg-red-100 p-2 rounded">
                    <strong>Customer {error.customerId}:</strong> {error.error}
                  </div>
                ))}
              </div>
            </div>
          )}

          {aggregatedData.length > 0 && (
            <div className="bg-gray-50 p-6 rounded-lg border">
              <h3 className="text-lg font-semibold mb-4">Data Preview</h3>
              <div className="overflow-x-auto max-h-80">
                <table className="min-w-full text-sm">
                  <thead className="bg-gray-100 sticky top-0">
                    <tr>
                      {Object.keys(aggregatedData[0] || {}).map(header => (
                        <th key={header} className="px-3 py-2 text-left font-medium text-gray-700 border-b">
                          {header}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {aggregatedData.slice(0, 50).map((row, index) => (
                      <tr key={index} className="hover:bg-gray-50">
                        {Object.values(row).map((value, cellIndex) => (
                          <td key={cellIndex} className="px-3 py-2 border-b text-gray-900">
                            {value}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {aggregatedData.length > 50 && (
                  <p className="text-center text-gray-500 mt-4">
                    Showing first 50 rows of {aggregatedData.length} total rows
                  </p>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;