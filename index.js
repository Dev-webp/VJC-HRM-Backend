const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const app = express();

// ✅ Step 1: Update CORS to match new frontend Render domain
app.use(cors({
  origin: 'https://postgres-frontend-attendance.onrender.com',
  credentials: true
}));

// ✅ Step 2: Enable body parsing
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// ✅ Step 3: Your routes
app.post('/', (req, res) => {
  const { email, password } = req.body;
  console.log('Login request:', email, password);

  // Simulate login
  if (email === 'admin@gmail.com' && password === '1234') {
    res.status(200).json({ message: 'Login success' });
  } else {
    res.status(401).json({ message: 'Invalid credentials' });
  }
});

app.get('/dashboard', (req, res) => {
  res.json({ redirect: 'chairman' }); // example
});

// ✅ Step 4: Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);
});
