// server.js
const express = require('express');
const multer = require('multer');
const path = require('path');
const cors = require('cors');
const fs = require('fs');
const { exec, spawn } = require('child_process');

const app = express();
const PORT = 3000;

// Enable CORS and serve static files from the 'public' directory
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const datasetPath = path.join(__dirname, '../common/Dataset');
const projectRoot = path.join(__dirname, '..');

if (!fs.existsSync(datasetPath)) {
    fs.mkdirSync(datasetPath, { recursive: true });
}

// API Endpoint to fetch existing pre-split datasets
app.get('/api/datasets', (req, res) => {
    try {
        const folders = fs.readdirSync(datasetPath, { withFileTypes: true })
            .filter(dirent => dirent.isDirectory())
            .map(dirent => dirent.name);
        res.json({ datasets: folders });
    } catch (error) {
        console.error("Error reading datasets:", error);
        res.status(500).json({ error: 'Failed to read dataset directory.' });
    }
});

// Configure Multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        // Create a unique subfolder inside Project/common/Dataset
        const folderName = 'upload_' + Date.now();
        const uploadPath = path.join(datasetPath, folderName);
        fs.mkdirSync(uploadPath, { recursive: true });
        
        // Pass the generated folder name in req so the endpoint can use it
        req.uploadedFolderName = folderName;
        cb(null, uploadPath);
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname); // Keep original filename
    }
});
const upload = multer({ storage: storage });

// API Endpoint to handle pipeline execution
app.post('/api/run-pipeline', upload.single('logfile'), (req, res) => {
    try {
        const method = req.body.method;
        const datasetMode = req.body.datasetMode; 
        
        let targetDataset = "";
        let batchSizeUsed = "N/A";

        let uploadFolder = "";
        
        if (datasetMode === 'pre_split') {
            targetDataset = req.body.datasetName;
            if (!targetDataset) return res.status(400).json({ error: 'No dataset selected.' });
        } else if (datasetMode === 'custom') {
            const file = req.file;
            batchSizeUsed = req.body.batchSize || 'Not Specified';
            if (!file) return res.status(400).json({ error: 'No log file uploaded.' });
            if (batchSizeUsed === 'Not Specified') return res.status(400).json({ error: 'Batch size is required for custom uploads.' });
            
            uploadFolder = req.uploadedFolderName; // Pass the raw upload folder to listener.sh
            
            // The frontend needs the final targetDataset to fetch the graph later!
            const nameWithoutExt = file.originalname.includes('.') 
                ? file.originalname.substring(0, file.originalname.lastIndexOf('.'))
                : file.originalname;
            targetDataset = `${nameWithoutExt}_${batchSizeUsed}`;
            
            console.log(`[UPLOAD] File saved at: ${file.path}`);
        } else {
            return res.status(400).json({ error: 'Invalid dataset mode.' });
        }

        console.log(`[PIPELINE] Triggered: ${method}`);
        console.log(`[PIPELINE] Dataset Mode: ${datasetMode}`);
        console.log(`[PIPELINE] Final Target Dataset: ${targetDataset}`);
        if (datasetMode === 'custom') console.log(`[PIPELINE] Batch Size: ${batchSizeUsed}`);

        const startTime = Date.now();
        // projectRoot is now global
        
        // Instead of running Java directly, we write to the Listener queue
        const queueFile = path.join(projectRoot, '.pipeline_queue');
        
        // We pass the raw uploadFolder to listener.sh if custom, otherwise the pre-split dataset
        const queueDataset = datasetMode === 'custom' ? uploadFolder : targetDataset;
        const commandData = `${queueDataset} ${method} ${batchSizeUsed}`;
        
        fs.writeFileSync(queueFile, commandData);
        console.log(`[QUEUE] Wrote command to listener queue: ${commandData}`);

        // Immediately respond to the frontend so it doesn't timeout!
        res.json({
            success: true,
            message: 'Pipeline initiated successfully. Please check your listener.sh terminal for live progress logs.',
            details: {
                methodRun: method,
                datasetModeUsed: datasetMode,
                targetDataset: targetDataset, // Frontend uses this to load the cross-pipeline graph!
                batchSizeUsed: batchSizeUsed,
                executionTimeMs: "RUNNING_IN_BACKGROUND",
                status: 'STARTED'
            }
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'An error occurred during pipeline execution.' });
    }
});

// Serve Analysis Graphs as static files
app.use('/graphs', express.static(path.join(__dirname, '../analysis/graphs')));

// API Endpoint to check the status of the background terminal listener
app.get('/api/pipeline-status', (req, res) => {
    const statusFile = path.join(projectRoot, '.pipeline_status');
    if (fs.existsSync(statusFile)) {
        const content = fs.readFileSync(statusFile, 'utf8').trim();
        const parts = content.split(' ');
        const status = parts[0];
        const id = parts[1] || null;
        
        // Clear the status file after reading so we don't repeat notifications
        fs.unlinkSync(statusFile);
        
        return res.json({ status, id });
    }
    res.json({ status: 'IDLE' });
});

// API Endpoint to trigger the Master Analyzer via the terminal listener
app.post('/api/run-analysis', (req, res) => {
    const executionId = req.body.id;
    if (!executionId) return res.status(400).json({ error: 'No execution ID provided.' });
    
    const queueFile = path.join(projectRoot, '.pipeline_queue');
    fs.writeFileSync(queueFile, `ANALYZE ${executionId}`);
    
    console.log(`[QUEUE] Triggered Analysis for ID: ${executionId}`);
    res.json({ success: true, message: 'Analysis triggered. Please check the terminal.' });
});

// API Endpoint to fetch run history
app.get('/api/history', (req, res) => {
    // We use the same listener logic to run the Java provider
    const cmd = `java -cp "target/classes:$(cat ../.classpath):$(hadoop classpath)" analysis.RunHistoryProvider`;
    
    exec(cmd, { cwd: projectRoot }, (error, stdout, stderr) => {
        if (error) {
            console.error("History Error:", stderr);
            return res.status(500).json({ error: 'Failed to fetch history' });
        }
        try {
            const history = JSON.parse(stdout);
            res.json({ history });
        } catch (e) {
            res.json({ history: [] });
        }
    });
});

app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
});
