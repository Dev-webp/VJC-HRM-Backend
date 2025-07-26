const express = require('express');
const cors = require('cors');
const app = express();

// ✅ Allow your Vercel frontend origin
app.use(cors({
  origin: 'https://postgres-frontend-attendance.vercel.app',
  credentials: true, // if using cookies or authorization headers
}));

// OR: Allow all origins (not recommended for production)
// app.use(cors());

app.listen(3000, () => {
  console.log('Server running on port 3000');
});
