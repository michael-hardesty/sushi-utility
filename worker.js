// Cloudlare Worker JS with SSE progress updates
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  const url = new URL(request.url)

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  }

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders })
  }

  try {
    if (url.pathname === '/status') {
      return new Response(JSON.stringify({
        status: 'healthy',
        timestamp: new Date().toISOString()
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      })
    }

    // SSE endpoint for progress streaming
    if (url.pathname === '/harvest-stream' && request.method === 'POST') {
      const requestData = await request.json()
      
      // Create a TransformStream for SSE
      const { readable, writable } = new TransformStream()
      const writer = writable.getWriter()
      const encoder = new TextEncoder()

      // Process in background
      processWithProgress(requestData, writer, encoder).then(() => {
        writer.close()
      }).catch(err => {
        writer.write(encoder.encode(`data: ${JSON.stringify({ error: err.message })}\n\n`))
        writer.close()
      })

      return new Response(readable, {
        headers: {
          ...corsHeaders,
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        }
      })
    }

    if (url.pathname === '/harvest-batch' && request.method === 'POST') {
      // Keep existing endpoint for backwards compatibility
      const requestData = await request.json()
      const result = await processAccounts(requestData)
      
      return new Response(JSON.stringify(result), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      })
    }

    return new Response('Not Found', { status: 404, headers: corsHeaders })

  } catch (error) {
    console.error('Request error:', error)
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    })
  }
}

async function processWithProgress(requestData, writer, encoder) {
  const results = []
  let successful = 0, failed = 0
  const totalAccounts = requestData.accounts.length
  const startTime = Date.now()

  for (let i = 0; i < requestData.accounts.length; i++) {
    const account = requestData.accounts[i]
    
    // Send progress update
    const progress = {
      type: 'progress',
      current: i + 1,
      total: totalAccounts,
      percentage: Math.round(((i + 1) / totalAccounts) * 100),
      currentAccount: account.customer_id,
      successful,
      failed,
      estimatedTimeRemaining: calculateETA(startTime, i + 1, totalAccounts)
    }
    
    await writer.write(encoder.encode(`data: ${JSON.stringify(progress)}\n\n`))

    try {
      const apiUrl = buildApiUrl(requestData, account)
      const response = await fetch(apiUrl)
      
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`)
      }
      
      const jsonData = await response.json()
      const processedData = processApiResponse(jsonData, requestData, account)
      
      results.push(...processedData.entries)
      if (processedData.success) {
        successful++
      } else {
        failed++
      }
      
    } catch (error) {
      console.error(`Error for ${account.customer_id}:`, error)
      failed++
      results.push(createErrorEntry(error, requestData, account))
    }
    
    await new Promise(r => setTimeout(r, 100))
  }

  // Send final results
  const csv = requestData.formatted
    ? formatAsPivot(results, requestData)
    : convertToCSV(results, requestData)
  const totalUsage = results.filter(r => r.Title !== 'ERROR')
    .reduce((sum, r) => sum + (parseInt(r.Reporting_Period_Total) || 0), 0)
  const platforms = [...new Set(results.map(r => r.Platform).filter(p => p))]
  const metricTypes = [...new Set(results.map(r => r.Metric_Type).filter(m => m && m !== 'ERROR' && m !== 'No Data'))]

  const finalResult = {
    type: 'complete',
    csv,
    filename: generateFilename(requestData) + (requestData.formatted ? '_formatted' : ''), // Add filename
    successful,
    failed,
    total: requestData.accounts.length,
    summary: {
      totalUsage,
      uniquePlatforms: platforms.length,
      metricTypes: metricTypes.slice(0, 5)
    }
  }

  await writer.write(encoder.encode(`data: ${JSON.stringify(finalResult)}\n\n`))
}

async function processAccounts(requestData) {
  // Existing logic for backward compatibility
  const results = []
  let successful = 0, failed = 0

  for (const account of requestData.accounts) {
    try {
      const apiUrl = buildApiUrl(requestData, account)
      const response = await fetch(apiUrl)
      
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`)
      }
      
      const jsonData = await response.json()
      const processedData = processApiResponse(jsonData, requestData, account)
      
      results.push(...processedData.entries)
      if (processedData.success) {
        successful++
      } else {
        failed++
      }
      
    } catch (error) {
      console.error(`Error for ${account.customer_id}:`, error)
      failed++
      results.push(createErrorEntry(error, requestData, account))
    }
    
    await new Promise(r => setTimeout(r, 100))
  }

  const csv = requestData.formatted
    ? formatAsPivot(results, requestData)
    : convertToCSV(results, requestData)
  const totalUsage = results.filter(r => r.Title !== 'ERROR')
    .reduce((sum, r) => sum + (parseInt(r.Reporting_Period_Total) || 0), 0)
  const platforms = [...new Set(results.map(r => r.Platform).filter(p => p))]
  const metricTypes = [...new Set(results.map(r => r.Metric_Type).filter(m => m && m !== 'ERROR' && m !== 'No Data'))]

  return {
    csv,
    filename: generateFilename(requestData) + (requestData.formatted ? '_formatted' : ''), // Add filename
    successful,
    failed,
    total: requestData.accounts.length,
    summary: {
      totalUsage,
      uniquePlatforms: platforms.length,
      metricTypes: metricTypes.slice(0, 5)
    }
  }
}

function buildApiUrl(requestData, account) {
  // Fix the URL structure for version 5.1
  const baseUrl = requestData.format === '5.1'
    ? 'https://www.pnas.org/r51/reports'
    : 'https://www.pnas.org/reports'

  const apiUrl = `${baseUrl}/${requestData.report_type}?` + new URLSearchParams({
    requestor_id: account.requestor_id,
    customer_id: account.customer_id,
    begin_date: requestData.begin_date,
    end_date: requestData.end_date
  })

  return apiUrl
}

function processApiResponse(jsonData, requestData, account) {
  const results = []
  const institutionName = jsonData.Report_Header?.Institution_Name || ''
  const institutionId = getInstitutionId(jsonData)
  const reportItems = jsonData.Report_Items || []
  
  // Get all month columns for the query period
  const monthColumns = getMonthColumns(requestData.begin_date, requestData.end_date)
  
  // Determine if this is version 5.1 format by checking structure
  const isVersion51 = requestData.format === '5.1' || 
    (reportItems.length > 0 && reportItems[0].Attribute_Performance)
  
  console.log(`Version detection: format=${requestData.format}, isVersion51=${isVersion51}, hasAttributePerformance=${reportItems.length > 0 && !!reportItems[0].Attribute_Performance}`)
  
  if (reportItems && reportItems.length > 0) {
    if (isVersion51) {
      console.log("Processing version 5.1 format")
      // Handle version 5.1 format
      for (const item of reportItems) {
        const baseData = extractItemData(item)
        
        // Process Attribute_Performance array
        for (const attrPerf of item.Attribute_Performance || []) {
          const accessType = attrPerf.Access_Type || ''
          const yop = attrPerf.YOP || '' // Extract YOP from Attribute_Performance
          const performance = attrPerf.Performance || {}
          
          // Process each metric type
          for (const [metricType, monthlyData] of Object.entries(performance)) {
            // Calculate total and collect monthly values
            const monthlyValues = {}
            let total = 0
            
            for (const [month, count] of Object.entries(monthlyData)) {
              const monthValue = parseInt(count) || 0
              total += monthValue
              // Convert "2025-01" format to "Jan-25" format
              const monthKey = convertApiMonthToDisplayMonth(month)
              monthlyValues[monthKey] = monthValue
            }
            
            const entry = {
              Institution_Name: institutionName,
              Institution_ID: institutionId,
              Title: baseData.title,
              Publisher: baseData.publisher,
              Publisher_ID: baseData.publisher_id,
              Platform: baseData.platform,
              DOI: baseData.doi,
              Proprietary_ID: baseData.proprietary_id,
              Print_ISSN: baseData.print_issn,
              Online_ISSN: baseData.online_issn,
              URI: baseData.uri,
              Metric_Type: metricType,
              Reporting_Period_Total: total
            }
            
            // Only add Access_Type for TR_J3
            if (requestData.report_type.toLowerCase() === 'tr_j3') {
              entry.Access_Type = accessType
            }
            
            // Add YOP for TR_J4
            if (requestData.report_type.toLowerCase() === 'tr_j4') {
              entry.YOP = yop
            }
            
            // Add monthly data ensuring all expected months are present
            monthColumns.forEach(month => {
              entry[month] = monthlyValues[month] || 0
            })

            results.push(entry)
          }
        }
      }
    } else {
      console.log("Processing version 5 format")
      // Handle version 5 format (original structure)
      for (const item of reportItems) {
        const baseData = extractItemData(item)
        const accessType = item.Access_Type || ''
        
        // Group all metric data by metric type across all Performance periods
        const metricData = {}
        
        // First pass: collect all metrics and their monthly values
        for (const performance of item.Performance || []) {
          const period = performance.Period
          if (!period) continue
          
          // Convert period to month key
          const monthKey = formatMonthKey(new Date(period.Begin_Date))
          
          for (const instance of performance.Instance || []) {
            const metricType = instance.Metric_Type
            const count = parseInt(instance.Count) || 0
            
            if (!metricData[metricType]) {
              metricData[metricType] = {
                monthlyValues: {},
                total: 0
              }
            }
            
            metricData[metricType].monthlyValues[monthKey] = count
            metricData[metricType].total += count
          }
        }
        
        // Second pass: create a row for each metric type with all monthly data
        for (const [metricType, data] of Object.entries(metricData)) {
          const entry = {
            Institution_Name: institutionName,
            Institution_ID: institutionId,
            Title: baseData.title,
            Publisher: baseData.publisher,
            Publisher_ID: baseData.publisher_id,
            Platform: baseData.platform,
            DOI: baseData.doi,
            Proprietary_ID: baseData.proprietary_id,
            Print_ISSN: baseData.print_issn,
            Online_ISSN: baseData.online_issn,
            URI: baseData.uri,
            Metric_Type: metricType,
            Reporting_Period_Total: data.total
          }

          // Only add Access_Type for TR_J3
          if (requestData.report_type.toLowerCase() === 'tr_j3') {
            entry.Access_Type = accessType
          }
          
          // Handle TR_J4 specific fields
          if (requestData.report_type.toLowerCase() === 'tr_j4') {
            entry.YOP = item.YOP || item.Year_of_Publication || ''
          }

          // Add monthly data for all months in the period
          monthColumns.forEach(month => {
            entry[month] = data.monthlyValues[month] || 0
          })

          results.push(entry)
        }
      }
    }
    return { entries: results, success: true }
  } else {
    // No data found
    const noDataEntry = {
      Institution_Name: institutionName || 'No Data',
      Institution_ID: institutionId || '',
      Title: 'No usage data for this period',
      Publisher: '',
      Publisher_ID: '',
      Platform: '',
      DOI: '',
      Proprietary_ID: '',
      Print_ISSN: '',
      Online_ISSN: '',
      URI: '',
      Metric_Type: 'No Data',
      Reporting_Period_Total: 0
    }
    
    // Only add Access_Type for TR_J3
    if (requestData.report_type.toLowerCase() === 'tr_j3') {
      noDataEntry.Access_Type = ''
    }
    
    // Add YOP for TR_J4
    if (requestData.report_type.toLowerCase() === 'tr_j4') {
      noDataEntry.YOP = ''
    }
    
    // Add monthly columns to no data entry
    monthColumns.forEach(month => {
      noDataEntry[month] = 0
    })
    return { entries: [noDataEntry], success: true }
  }
}

function getInstitutionId(jsonData) {
  const institutionIds = jsonData.Report_Header?.Institution_ID
  
  if (!institutionIds) return ''
  
  if (Array.isArray(institutionIds)) {
    // Version 5 format - array of objects
    const proprietaryId = institutionIds.find(id => id.Type === 'Proprietary')
    return proprietaryId?.Value || ''
  } else {
    // Version 5.1 format - object with properties
    return institutionIds.Proprietary?.[0] || institutionIds.Proprietary || ''
  }
}

function extractItemData(item) {
  // Extract item identifiers
  const itemIds = item.Item_ID || {}
  
  // Handle both array and object formats for Item_ID
  let doi = '', proprietary_id = '', print_issn = '', online_issn = ''
  
  if (Array.isArray(itemIds)) {
    // Version 5 format - array of objects
    for (const id of itemIds) {
      switch (id.Type) {
        case 'DOI': doi = id.Value || ''; break
        case 'Proprietary': proprietary_id = id.Value || ''; break
        case 'Print_ISSN': print_issn = id.Value || ''; break
        case 'Online_ISSN': online_issn = id.Value || ''; break
      }
    }
  } else {
    // Version 5.1 format - object with properties
    doi = itemIds.DOI || ''
    proprietary_id = itemIds.Proprietary || ''
    print_issn = itemIds.Print_ISSN || ''
    online_issn = itemIds.Online_ISSN || ''
  }
  
  // Extract publisher ID
  const publisherIds = item.Publisher_ID || {}
  let publisher_id = ''
  
  if (Array.isArray(publisherIds)) {
    const proprietaryPub = publisherIds.find(p => p.Type === 'Proprietary')
    publisher_id = proprietaryPub?.Value || ''
  } else {
    publisher_id = publisherIds.Proprietary?.[0] || publisherIds.Proprietary || ''
  }
  
  return {
    title: item.Title || '',
    publisher: item.Publisher || '',
    publisher_id,
    platform: item.Platform || '',
    doi,
    proprietary_id,
    print_issn,
    online_issn,
    uri: '' // URI field seems to be empty in the sample
  }
}

function convertApiMonthToDisplayMonth(apiMonth) {
  // Convert "2025-01" to "Jan-25"
  const [year, monthNum] = apiMonth.split('-')
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  const monthName = months[parseInt(monthNum) - 1]
  const shortYear = year.slice(-2)
  return `${monthName}-${shortYear}`
}

function formatMonthKey(date) {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  const month = months[date.getMonth()]
  const year = date.getFullYear().toString().slice(-2)
  return `${month}-${year}`
}

function getMonthColumns(beginDate, endDate) {
  const months = []
  const start = new Date(beginDate)
  const end = new Date(endDate)
  
  const current = new Date(start.getFullYear(), start.getMonth(), 1)
  
  while (current <= end) {
    months.push(formatMonthKey(current))
    current.setMonth(current.getMonth() + 1)
  }
  
  return months
}

function createErrorEntry(error, requestData, account) {
  const monthColumns = getMonthColumns(requestData.begin_date, requestData.end_date)
  
  const errorEntry = {
    Institution_Name: 'ERROR',
    Institution_ID: account.customer_id,
    Title: `Failed: ${error.message}`,
    Publisher: '',
    Publisher_ID: '',
    Platform: '',
    DOI: '',
    Proprietary_ID: '',
    Print_ISSN: '',
    Online_ISSN: '',
    URI: '',
    Metric_Type: 'ERROR',
    Reporting_Period_Total: 0
  }
  
  // Only add Access_Type for TR_J3
  if (requestData.report_type.toLowerCase() === 'tr_j3') {
    errorEntry.Access_Type = ''
  }
  
  // Add YOP for TR_J4
  if (requestData.report_type.toLowerCase() === 'tr_j4') {
    errorEntry.YOP = ''
  }
  
  // Add monthly columns to error entry
  monthColumns.forEach(month => {
    errorEntry[month] = 0
  })
  
  return errorEntry
}

function generateFilename(requestData) {
  // Format: bulk_counter_{version}-{report-type}_{date}
  const version = requestData.format || '5'
  const reportType = requestData.report_type.toLowerCase()
  const date = new Date().toISOString().split('T')[0].replace(/-/g, '')
  
  return `bulk_counter_${version}-${reportType}_${date}`
}

function calculateETA(startTime, processed, total) {
  const elapsed = Date.now() - startTime
  const averageTime = elapsed / processed
  const remaining = total - processed
  const eta = remaining * averageTime
  
  // Convert to human readable format
  if (eta < 60000) {
    return `${Math.round(eta / 1000)} seconds`
  } else {
    return `${Math.round(eta / 60000)} minutes`
  }
}

function formatAsPivot(data, requestData) {
  const monthColumns = getMonthColumns(requestData.begin_date, requestData.end_date)
  const reportType = requestData.report_type.toLowerCase()

  // Filter out error rows for pivot formatting
  const validData = data.filter(r => r.Title !== 'ERROR' && r.Metric_Type !== 'No Data')
  const errorData = data.filter(r => r.Title === 'ERROR' || r.Metric_Type === 'No Data')

  // Group data by institution
  const byInstitution = {}
  for (const row of validData) {
    const instName = row.Institution_Name
    if (!byInstitution[instName]) {
      byInstitution[instName] = []
    }
    byInstitution[instName].push(row)
  }

  const pivotRows = []
  const grandTotals = { total: 0 }
  monthColumns.forEach(m => grandTotals[m] = 0)

  // Sort institutions alphabetically
  const sortedInstitutions = Object.keys(byInstitution).sort()

  for (const instName of sortedInstitutions) {
    const instRows = byInstitution[instName]
    const instTotals = { total: 0 }
    monthColumns.forEach(m => instTotals[m] = 0)

    // Determine grouping based on report type
    let groupedData = {}

    if (reportType === 'tr_j3') {
      // Group by Access_Type, then Metric_Type
      for (const row of instRows) {
        const accessType = row.Access_Type || 'Unknown'
        if (!groupedData[accessType]) {
          groupedData[accessType] = {}
        }
        const metricType = row.Metric_Type
        if (!groupedData[accessType][metricType]) {
          groupedData[accessType][metricType] = { total: 0 }
          monthColumns.forEach(m => groupedData[accessType][metricType][m] = 0)
        }
        groupedData[accessType][metricType].total += parseInt(row.Reporting_Period_Total) || 0
        monthColumns.forEach(m => {
          groupedData[accessType][metricType][m] += parseInt(row[m]) || 0
        })
      }

      // Calculate institution totals and build pivot rows
      const accessTypeOrder = ['Controlled', 'Free_To_Read', 'Open']
      const metricOrder = ['Total_Item_Investigations', 'Total_Item_Requests', 'Unique_Item_Investigations', 'Unique_Item_Requests']

      for (const accessType of accessTypeOrder) {
        if (!groupedData[accessType]) continue

        const accessTotals = { total: 0 }
        monthColumns.forEach(m => accessTotals[m] = 0)

        for (const metricType of metricOrder) {
          if (!groupedData[accessType][metricType]) continue
          const metricData = groupedData[accessType][metricType]
          accessTotals.total += metricData.total
          monthColumns.forEach(m => accessTotals[m] += metricData[m])
        }

        instTotals.total += accessTotals.total
        monthColumns.forEach(m => instTotals[m] += accessTotals[m])
      }

      // Add institution header row
      pivotRows.push({
        label: instName,
        level: 0,
        total: instTotals.total,
        months: monthColumns.map(m => instTotals[m])
      })

      // Add access type and metric rows
      for (const accessType of accessTypeOrder) {
        if (!groupedData[accessType]) continue

        const accessTotals = { total: 0 }
        monthColumns.forEach(m => accessTotals[m] = 0)

        for (const metricType of metricOrder) {
          if (!groupedData[accessType][metricType]) continue
          const metricData = groupedData[accessType][metricType]
          accessTotals.total += metricData.total
          monthColumns.forEach(m => accessTotals[m] += metricData[m])
        }

        pivotRows.push({
          label: accessType,
          level: 1,
          total: accessTotals.total,
          months: monthColumns.map(m => accessTotals[m])
        })

        for (const metricType of metricOrder) {
          if (!groupedData[accessType][metricType]) continue
          const metricData = groupedData[accessType][metricType]
          pivotRows.push({
            label: metricType,
            level: 2,
            total: metricData.total,
            months: monthColumns.map(m => metricData[m])
          })
        }
      }

    } else if (reportType === 'tr_j4') {
      // Group by YOP, then Metric_Type
      for (const row of instRows) {
        const yop = row.YOP || 'Unknown'
        if (!groupedData[yop]) {
          groupedData[yop] = {}
        }
        const metricType = row.Metric_Type
        if (!groupedData[yop][metricType]) {
          groupedData[yop][metricType] = { total: 0 }
          monthColumns.forEach(m => groupedData[yop][metricType][m] = 0)
        }
        groupedData[yop][metricType].total += parseInt(row.Reporting_Period_Total) || 0
        monthColumns.forEach(m => {
          groupedData[yop][metricType][m] += parseInt(row[m]) || 0
        })
      }

      const metricOrder = ['Total_Item_Investigations', 'Total_Item_Requests', 'Unique_Item_Investigations', 'Unique_Item_Requests']

      // Calculate institution totals
      for (const yop of Object.keys(groupedData)) {
        for (const metricType of metricOrder) {
          if (!groupedData[yop][metricType]) continue
          const metricData = groupedData[yop][metricType]
          instTotals.total += metricData.total
          monthColumns.forEach(m => instTotals[m] += metricData[m])
        }
      }

      // Add institution header row
      pivotRows.push({
        label: instName,
        level: 0,
        total: instTotals.total,
        months: monthColumns.map(m => instTotals[m])
      })

      // Sort YOP values (most recent first)
      const sortedYops = Object.keys(groupedData).sort((a, b) => {
        if (a === 'Unknown') return 1
        if (b === 'Unknown') return -1
        return parseInt(b) - parseInt(a)
      })

      for (const yop of sortedYops) {
        const yopTotals = { total: 0 }
        monthColumns.forEach(m => yopTotals[m] = 0)

        for (const metricType of metricOrder) {
          if (!groupedData[yop][metricType]) continue
          const metricData = groupedData[yop][metricType]
          yopTotals.total += metricData.total
          monthColumns.forEach(m => yopTotals[m] += metricData[m])
        }

        pivotRows.push({
          label: yop,
          level: 1,
          total: yopTotals.total,
          months: monthColumns.map(m => yopTotals[m])
        })

        for (const metricType of metricOrder) {
          if (!groupedData[yop][metricType]) continue
          const metricData = groupedData[yop][metricType]
          pivotRows.push({
            label: metricType,
            level: 2,
            total: metricData.total,
            months: monthColumns.map(m => metricData[m])
          })
        }
      }

    } else {
      // TR_J1, TR_J2: Group by Metric_Type only
      for (const row of instRows) {
        const metricType = row.Metric_Type
        if (!groupedData[metricType]) {
          groupedData[metricType] = { total: 0 }
          monthColumns.forEach(m => groupedData[metricType][m] = 0)
        }
        groupedData[metricType].total += parseInt(row.Reporting_Period_Total) || 0
        monthColumns.forEach(m => {
          groupedData[metricType][m] += parseInt(row[m]) || 0
        })
      }

      // Calculate institution totals
      for (const metricType of Object.keys(groupedData)) {
        instTotals.total += groupedData[metricType].total
        monthColumns.forEach(m => instTotals[m] += groupedData[metricType][m])
      }

      // Add institution header row
      pivotRows.push({
        label: instName,
        level: 0,
        total: instTotals.total,
        months: monthColumns.map(m => instTotals[m])
      })

      const metricOrder = ['Total_Item_Investigations', 'Total_Item_Requests', 'Unique_Item_Investigations', 'Unique_Item_Requests']
      for (const metricType of metricOrder) {
        if (!groupedData[metricType]) continue
        const metricData = groupedData[metricType]
        pivotRows.push({
          label: metricType,
          level: 1,
          total: metricData.total,
          months: monthColumns.map(m => metricData[m])
        })
      }
    }

    // Add to grand totals
    grandTotals.total += instTotals.total
    monthColumns.forEach(m => grandTotals[m] += instTotals[m])
  }

  // Add Grand Total row
  pivotRows.push({
    label: 'Grand Total',
    level: 0,
    total: grandTotals.total,
    months: monthColumns.map(m => grandTotals[m])
  })

  // Convert to CSV
  const headers = ['', 'Reporting_Period_Total', ...monthColumns]
  const csvRows = [headers.map(h => `"${h}"`).join(',')]

  for (const row of pivotRows) {
    // Add indentation based on level
    const indent = '  '.repeat(row.level)
    const values = [
      `"${indent}${row.label}"`,
      row.total,
      ...row.months
    ]
    csvRows.push(values.join(','))
  }

  // Add error rows at the end if any
  if (errorData.length > 0) {
    csvRows.push('') // blank row
    csvRows.push('"--- Errors ---"')
    for (const row of errorData) {
      csvRows.push(`"${row.Institution_ID}","${row.Title}"`)
    }
  }

  return csvRows.join('\n')
}

function convertToCSV(data, requestData) {
  if (!data.length) return ''
  
  // Define the base columns in the correct order
  const baseColumns = [
    'Institution_Name', 'Institution_ID', 'Title', 'Publisher', 'Publisher_ID', 'Platform', 'DOI', 
    'Proprietary_ID', 'Print_ISSN', 'Online_ISSN', 'URI'
  ]
  
  // Add YOP column for TR_J4 reports (before Access_Type)
  if (requestData.report_type.toLowerCase() === 'tr_j4') {
    baseColumns.push('YOP')
  }
  
  // Add Access_Type column only for TR_J3 reports
  if (requestData.report_type.toLowerCase() === 'tr_j3') {
    baseColumns.push('Access_Type')
  }
  
  // Add Metric_Type and Reporting_Period_Total
  baseColumns.push('Metric_Type', 'Reporting_Period_Total')
  
  // Get month columns for the query period directly
  const monthColumns = getMonthColumns(requestData.begin_date, requestData.end_date)
  
  // Combine all columns
  const headers = [...baseColumns, ...monthColumns]
  
  // Create CSV
  const csvRows = [headers.map(h => `"${h}"`).join(',')]
  
  for (const row of data) {
    const values = headers.map(h => {
      const v = row[h]
      return (v === null || v === undefined) ? '""' : `"${String(v).replace(/"/g, '""')}"`
    })
    csvRows.push(values.join(','))
  }
  
  return csvRows.join('\n')
}