const express = require('express');
const Database = require('better-sqlite3');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;

app.use(bodyParser.json());

// Connect to SQLite database
const db = new Database('./project123.db', { verbose: console.log });

// Create tables if they do not exist
db.exec(`
  CREATE TABLE IF NOT EXISTS PendingOrderTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_qty INTEGER,
    buyer_price REAL,
    seller_price REAL,
    seller_qty INTEGER
  )
`);
db.exec(`
  CREATE TABLE IF NOT EXISTS CompletedOrderTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price REAL,
    qty INTEGER
  )
`);

// Function to match orders
function matchOrders() {
  db.transaction(() => {
    const rows = db.prepare('SELECT * FROM PendingOrderTable WHERE seller_price IS NOT NULL AND buyer_price >= seller_price').all();

    rows.forEach(row => {
      const matched_qty = Math.min(row.buyer_qty || 0, row.seller_qty || 0);
      const remaining_buyer_qty = (row.buyer_qty || 0) - matched_qty;
      const remaining_seller_qty = (row.seller_qty || 0) - matched_qty;

      // Insert matching orders into CompletedOrderTable
      db.prepare('INSERT INTO CompletedOrderTable (price, qty) VALUES (?, ?)').run(row.seller_price, matched_qty);

      // Update or delete pending orders based on remaining quantities
      if (remaining_buyer_qty > 0) {
        db.prepare('UPDATE PendingOrderTable SET buyer_qty = ?, seller_price = NULL, seller_qty = NULL WHERE id = ?').run(remaining_buyer_qty, row.id);
      } else if (remaining_seller_qty > 0) {
        db.prepare('UPDATE PendingOrderTable SET seller_qty = ? WHERE id = ?').run(remaining_seller_qty, row.id);
      } else {
        db.prepare('DELETE FROM PendingOrderTable WHERE id = ?').run(row.id);
      }
    });
  })();
}

// API to add an order
app.post('/order', (req, res) => {
  const { buyer_qty, buyer_price, seller_price, seller_qty } = req.body;

  if (buyer_price && buyer_qty) {
    // Insert or update a buyer order
    db.prepare(`INSERT INTO PendingOrderTable (buyer_qty, buyer_price, seller_price, seller_qty) VALUES (?, ?, NULL, NULL)`).run(buyer_qty, buyer_price);
    matchOrders();
    res.json({ message: 'Buyer order added to pending orders' });
  } else if (seller_price && seller_qty) {
    // Find a matching buyer order to update
    const row = db.prepare(`SELECT * FROM PendingOrderTable WHERE seller_price IS NULL AND seller_qty IS NULL AND buyer_price >= ? AND buyer_qty >= ?`).get(seller_price, seller_qty);
    
    if (row) {
      // Update the existing row with seller information
      db.prepare(`UPDATE PendingOrderTable SET seller_price = ?, seller_qty = ? WHERE id = ?`).run(seller_price, seller_qty, row.id);
      matchOrders();
      res.json({ message: 'Seller order matched and updated in pending orders' });
    } else {
      // If no match, insert a new seller order
      db.prepare(`INSERT INTO PendingOrderTable (buyer_qty, buyer_price, seller_price, seller_qty) VALUES (NULL, NULL, ?, ?)`).run(seller_price, seller_qty);
      res.json({ message: 'Seller order added to pending orders' });
    }
  } else {
    res.status(400).json({ message: 'Invalid order data' });
  }
});

// Get Orders Endpoint
app.get('/orders', (req, res) => {
  db.transaction(() => {
    const rows = db.prepare('SELECT * FROM PendingOrderTable').all();
    const completedRows = db.prepare('SELECT * FROM CompletedOrderTable').all();

    // Transform the rows to handle filling of empty columns
    const filledPendingOrders = [];
    rows.forEach(row => {
      const existingRow = filledPendingOrders.find(order =>
        order.buyer_qty === row.buyer_qty &&
        order.buyer_price === row.buyer_price &&
        (order.seller_price === null || order.seller_qty === null || row.seller_price !== null || row.seller_qty !== null)
      );

      if (existingRow) {
        if (row.seller_price) {
          existingRow.seller_price = row.seller_price;
        }
        if (row.seller_qty) {
          existingRow.seller_qty = row.seller_qty;
        }
      } else {
        filledPendingOrders.push({
          buyer_qty: row.buyer_qty,
          buyer_price: row.buyer_price,
          seller_price: row.seller_price,
          seller_qty: row.seller_qty
        });
      }
    });

    res.json({
      pendingOrders: filledPendingOrders,
      completedOrders: completedRows
    });
  })();
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
