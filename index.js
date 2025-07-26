const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const app = express();

// ✅ 1. Enable CORS for your frontend
app.use(cors({
  origin: 'https://postgres-frontend-attendance.vercel.app',
  credentials: true
}));

// ✅ 2. Parse URL-encoded and JSON body
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// ✅ 3. Simulated session/auth check (replace with your logic)
let dummySession = {};

// ✅ 4. Login Route
app.post('/', (req, res) => {
  const { email, password } = req.body;
  console.log('Received login:', email, password);

  // Simulate login logic
  if (email === 'admin@gmail.com' && password === '1234') {
    dummySession[email] = 'chairman'; // or employee
    res.status(200).json({ message: 'Login successful' });
  } else {
    res.status(401).json({ message: 'Invalid credentials' });
  }
});

// ✅ 5. Dashboard route
app.get('/dashboard', (req, res) => {
  const userEmail = Object.keys(dummySession)[0]; // simulate session
  const role = dummySession[userEmail];

  if (role) {
    res.json({ redirect: role });
  } else {
    res.status(403).json({ message: 'Not logged in' });
  }
});

// ✅ 6. Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
