// public/app.js
document.addEventListener('DOMContentLoaded', () => {

    // --- Element References ---
    const form            = document.getElementById('pipelineForm');
    const fileInput       = document.getElementById('logfile');
    const fileDropArea    = document.getElementById('fileDropArea');
    const dropBadge       = document.getElementById('dropBadge');
    const submitBtn       = document.getElementById('submitBtn');
    const btnText         = document.getElementById('btnText');
    const loader          = document.getElementById('loader');
    const resultsPanel    = document.getElementById('resultsPanel');
    const resultContent   = document.getElementById('resultContent');
    const datasetSelect   = document.getElementById('datasetName');
    const datasetModeInput = document.getElementById('datasetModeInput');

    const togglePresplit  = document.getElementById('togglePresplit');
    const toggleCustom    = document.getElementById('toggleCustom');
    const preSplitSection = document.getElementById('preSplitSection');
    const customSection   = document.getElementById('customFileSection');

    const btnViewAnalysis = document.getElementById('btnViewAnalysis');
    const btnReset        = document.getElementById('btnReset');
    
    // Modal References
    const analysisModal   = document.getElementById('analysisModal');
    const closeModalBtn   = document.getElementById('closeModalBtn');
    const modalChartImg   = document.getElementById('modalChartImg');
    const chartLoader     = document.getElementById('chartLoader');
    const navBtns         = document.querySelectorAll('.nav-btn');

    let currentExecutionId = null;
    let currentDatasetName = null;
    let pollInterval = null;

    // --- History Section References ---
    const historyList    = document.getElementById('historyList');
    const refreshHistory = document.getElementById('refreshHistory');

    // --- Fetch Available Datasets ---
    function fetchDatasets() {
        fetch('/api/datasets')
            .then(res => res.json())
            .then(data => {
                datasetSelect.innerHTML = '<option value="" disabled selected>Select a dataset...</option>';
                if (data.datasets && data.datasets.length > 0) {
                    data.datasets.forEach(ds => {
                        const opt = document.createElement('option');
                        opt.value = ds;
                        opt.textContent = `📁 ${ds}`;
                        datasetSelect.appendChild(opt);
                    });
                } else {
                    datasetSelect.innerHTML = '<option value="" disabled selected>No datasets found</option>';
                }
            });
    }

    // --- Fetch and Render History ---
    function fetchHistory() {
        historyList.innerHTML = '<div class="history-empty">Loading history...</div>';
        fetch('/api/history')
            .then(res => res.json())
            .then(data => {
                historyList.innerHTML = '';
                if (data.history && data.history.length > 0) {
                    data.history.forEach(run => {
                        const item = document.createElement('div');
                        item.className = 'history-item';
                        
                        const badgeClass = `h-badge-${run.pipeline.toLowerCase()}`;
                        const date = new Date(run.timestamp).toLocaleDateString('en-US', { 
                            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' 
                        });

                        item.innerHTML = `
                            <div class="history-info">
                                <div class="h-dataset">📁 ${run.dataset}</div>
                                <div class="h-meta">
                                    <span class="h-badge ${badgeClass}">${run.pipeline}</span>
                                    <span>• ID: ${run.id}</span>
                                    <span>• ${date}</span>
                                </div>
                            </div>
                            <button class="h-analyze-btn" data-id="${run.id}" data-dataset="${run.dataset}">
                                📊 Analyze Results
                            </button>
                        `;
                        historyList.appendChild(item);
                    });

                    // Add click listeners to new buttons
                    document.querySelectorAll('.h-analyze-btn').forEach(btn => {
                        btn.addEventListener('click', () => {
                            const runId = btn.dataset.id;
                            const dataset = btn.dataset.dataset;
                            triggerHistoricalAnalysis(runId, dataset, btn);
                        });
                    });
                } else {
                    historyList.innerHTML = '<div class="history-empty">No previous runs found in database.</div>';
                }
            })
            .catch(() => {
                historyList.innerHTML = '<div class="history-empty">Failed to load history.</div>';
            });
    }

    async function triggerHistoricalAnalysis(runId, dataset, btnElement) {
        const originalText = btnElement.innerHTML;
        btnElement.innerHTML = "⏳ Processing...";
        btnElement.disabled = true;

        currentExecutionId = runId;
        currentDatasetName = dataset;

        try {
            await fetch('/api/run-analysis', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: runId })
            });

            let analysisPoll = setInterval(async () => {
                const res = await fetch('/api/pipeline-status');
                const data = await res.json();
                if (data.status === 'COMPLETED_ANALYSIS') {
                    clearInterval(analysisPoll);
                    openModal();
                    btnElement.innerHTML = originalText;
                    btnElement.disabled = false;
                }
            }, 1000);
        } catch (e) {
            showToast('Analysis failed!');
            btnElement.innerHTML = originalText;
            btnElement.disabled = false;
        }
    }

    refreshHistory.addEventListener('click', fetchHistory);

    // Initial Load
    fetchDatasets();
    fetchHistory();

    // --- Toggle Dataset Mode ---
    function setMode(mode) {
        datasetModeInput.value = mode;
        if (mode === 'pre_split') {
            togglePresplit.classList.add('active');
            toggleCustom.classList.remove('active');
            preSplitSection.classList.remove('fade-hidden');
            customSection.classList.add('fade-hidden');
        } else {
            toggleCustom.classList.add('active');
            togglePresplit.classList.remove('active');
            customSection.classList.remove('fade-hidden');
            preSplitSection.classList.add('fade-hidden');
        }
    }

    togglePresplit.addEventListener('click', () => setMode('pre_split'));
    toggleCustom.addEventListener('click',   () => setMode('custom'));

    // --- Drag & Drop ---
    fileInput.addEventListener('change', () => {
        if (fileInput.files && fileInput.files.length > 0) {
            dropBadge.textContent = `✓ ${fileInput.files[0].name}`;
            dropBadge.classList.add('has-file');
        }
    });

    // --- Form Submission ---
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const mode = datasetModeInput.value;
        const pipeline = document.querySelector('input[name="pipelineEngine"]:checked');

        if (!pipeline) return showToast('Select an engine!');
        
        btnText.classList.add('hidden');
        loader.classList.remove('hidden');
        submitBtn.disabled = true;
        resultsPanel.classList.add('hidden');

        const formData = new FormData();
        formData.append('method', pipeline.value);
        formData.append('datasetMode', mode);
        formData.append('querySelect', document.getElementById('querySelect').value);
        if (mode === 'pre_split') {
            formData.append('datasetName', datasetSelect.value);
        } else {
            formData.append('logfile', fileInput.files[0]);
            formData.append('batchSize', document.getElementById('batchSize').value);
        }

        try {
            const response = await fetch('/api/run-pipeline', { method: 'POST', body: formData });
            const data = await response.json();
            showToast(data.message);
            
            if (data.details && data.details.targetDataset) {
                currentDatasetName = data.details.targetDataset;
            }
            
            // Start Polling for completion
            startStatusPolling();
        } catch {
            showToast('Connection error!');
            resetBtn();
        }
    });

    function startStatusPolling() {
        if (pollInterval) clearInterval(pollInterval);
        pollInterval = setInterval(async () => {
            try {
                const res = await fetch('/api/pipeline-status');
                const data = await res.json();
                
                if (data.status === 'COMPLETED_PIPELINE') {
                    clearInterval(pollInterval);
                    currentExecutionId = data.id;
                    displayPipelineFinished(data.id);
                    resetBtn();
                }
            } catch (e) {}
        }, 2000);
    }

    function displayPipelineFinished(id) {
        resultsPanel.classList.remove('hidden');
        resultContent.innerHTML = `
            <div class="result-item result-wide">
                <div class="result-key">LATEST EXECUTION SUCCESSFUL</div>
                <div class="result-val" style="color:var(--accent-green)">Execution ID: ${id}</div>
            </div>
            <div class="result-item result-wide">
                <div class="result-key">STATUS</div>
                <div class="result-val">Pipeline finished in terminal. Click View Analysis to explore performance and quality charts.</div>
            </div>
        `;
        resultsPanel.scrollIntoView({ behavior: 'smooth' });
    }

    // --- View Analysis ---
    btnViewAnalysis.addEventListener('click', async () => {
        if (!currentExecutionId) return;
        
        btnViewAnalysis.textContent = "⏳ Analyzing...";
        btnViewAnalysis.disabled = true;

        try {
            // 1. Trigger the Master Analyzer
            await fetch('/api/run-analysis', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: currentExecutionId })
            });

            // 2. Poll for Analysis completion
            let analysisPoll = setInterval(async () => {
                const res = await fetch('/api/pipeline-status');
                const data = await res.json();
                if (data.status === 'COMPLETED_ANALYSIS') {
                    clearInterval(analysisPoll);
                    openModal();
                    btnViewAnalysis.textContent = "📊 View Analysis";
                    btnViewAnalysis.disabled = false;
                }
            }, 1000);

        } catch (e) {
            showToast('Analysis failed!');
            btnViewAnalysis.textContent = "📊 View Analysis";
            btnViewAnalysis.disabled = false;
        }
    });

    // --- Modal Logic ---
    function openModal() {
        analysisModal.classList.remove('hidden');
        // Reset nav to first item
        navBtns.forEach(b => b.classList.remove('active'));
        navBtns[0].classList.add('active');
        loadChart('quality'); 
    }

    closeModalBtn.addEventListener('click', () => {
        analysisModal.classList.add('hidden');
    });

    navBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            navBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            loadChart(btn.dataset.chart);
        });
    });

    function loadChart(type) {
        chartLoader.classList.remove('hidden');
        modalChartImg.classList.add('hidden');
        
        const modalChartImg2 = document.getElementById('modalChartImg2');
        const modalChartImg3 = document.getElementById('modalChartImg3');
        if (modalChartImg2) modalChartImg2.classList.add('hidden');
        if (modalChartImg3) modalChartImg3.classList.add('hidden');
        
        let src = '';
        let src2 = '';
        let src3 = '';
        const id = currentExecutionId;
        const ds = currentDatasetName ? currentDatasetName.replace(/\//g, "_") : "";
        const baseDs = ds.includes('_') ? ds.substring(0, ds.lastIndexOf('_')) : ds;

        if (type === 'quality') src = `/graphs/DataQuality/quality_chart_${id}.png`;
        else if (type === 'validation') src = `/graphs/Validation/validation_chart_${id}.png`;
        else if (type === 'pipeline') src = `/graphs/PipelineComparison/runtime_comparison_${id}.png`;
        else if (type === 'queryTotal') src = `/graphs/Performance/query_comparison_${id}.png`;
        else if (type === 'batch') src = `/graphs/Performance/batch_comparison_${id}.png`;
        else if (type === 'crossPipeline') src = `/graphs/Performance/pipeline_comparison_${ds}.png`;
        else if (type === 'batchImpact') {
            src = `/graphs/BatchSizeAnalyzer/batch_size_runtime_${baseDs}.png`;
            src2 = `/graphs/BatchSizeAnalyzer/batch_size_avgtime_${baseDs}.png`;
        }

        modalChartImg.src = src + `?t=${Date.now()}`;
        
        let loadedCount = 0;
        const targetCount = type === 'batchImpact' ? 2 : 1;
        
        modalChartImg.onload = () => {
            loadedCount++;
            if (loadedCount === targetCount) chartLoader.classList.add('hidden');
            modalChartImg.classList.remove('hidden');
        };
        
        modalChartImg.onerror = () => {
            chartLoader.classList.add('hidden');
            if (type !== 'batchImpact') showToast('Chart image not found.');
            else showToast('Run this dataset with different batch sizes to unlock this analysis!');
        };
        
        if (type === 'batchImpact' && modalChartImg2) {
            modalChartImg2.src = src2 + `?t=${Date.now()}`;
            
            modalChartImg2.onload = () => {
                loadedCount++;
                if (loadedCount === targetCount) chartLoader.classList.add('hidden');
                modalChartImg2.classList.remove('hidden');
            };
        }
    }

    btnReset.addEventListener('click', () => {
        resultsPanel.classList.add('hidden');
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    function resetBtn() {
        btnText.classList.remove('hidden');
        loader.classList.add('hidden');
        submitBtn.disabled = false;
    }

    function showToast(msg) {
        let toast = document.querySelector('.toast');
        if (!toast) {
            toast = document.createElement('div');
            toast.className = 'toast';
            document.body.appendChild(toast);
        }
        toast.textContent = msg;
        toast.classList.add('visible');
        setTimeout(() => toast.classList.remove('visible'), 3500);
    }
});
