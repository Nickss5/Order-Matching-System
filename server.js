const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bodyParser = require('body-parser');
const app = express();
const port = 3000;

app.use(bodyParser.json());

// Connect to SQLite database
const db = new sqlite3.Database('./project123.db', (err) => {
  if (err) {
    console.error('Error opening database:', err.message);
  } else {
    console.log('Connected to the SQLite database.');
    db.run(`
      CREATE TABLE IF NOT EXISTS PendingOrderTable (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        buyer_qty INTEGER,
        buyer_price REAL,
        seller_price REAL,
        seller_qty INTEGER
      )
    `);
    db.run(`
      CREATE TABLE IF NOT EXISTS CompletedOrderTable (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        price REAL,
        qty INTEGER
      )
    `);
  }
});

// Function to match orders
function matchOrders() {
  db.serialize(() => {
    db.all('SELECT * FROM PendingOrderTable WHERE seller_price IS NOT NULL AND buyer_price >= seller_price', [], (err, rows) => {
      if (err) {
        return console.error(err.message);
      }

      rows.forEach(row => {
        const matched_qty = Math.min(row.buyer_qty || 0, row.seller_qty || 0);
        const remaining_buyer_qty = (row.buyer_qty || 0) - matched_qty;
        const remaining_seller_qty = (row.seller_qty || 0) - matched_qty;

        // Insert matching orders into CompletedOrderTable
        db.run('INSERT INTO CompletedOrderTable (price, qty) VALUES (?, ?)',
          [row.seller_price, matched_qty],
          function (err) {
            if (err) {
              return console.error(err.message);
            }

            // Update or delete pending orders based on remaining quantities
            if (remaining_buyer_qty > 0) {
              db.run('UPDATE PendingOrderTable SET buyer_qty = ?, seller_price = NULL, seller_qty = NULL WHERE id = ?',
                [remaining_buyer_qty, row.id],
                function (err) {
                  if (err) {
                    return console.error(err.message);
                  }
                }
              );
            } else if (remaining_seller_qty > 0) {
              db.run('UPDATE PendingOrderTable SET seller_qty = ? WHERE id = ?',
                [remaining_seller_qty, row.id],
                function (err) {
                  if (err) {
                    return console.error(err.message);
                  }
                }
              );
            } else {
              db.run('DELETE FROM PendingOrderTable WHERE id = ?', row.id, function (err) {
                if (err) {
                  return console.error(err.message);
                }
              });
            }
          }
        );
      });
    });
  });
}

// API to add an order
app.post('/order', (req, res) => {
  const { buyer_qty, buyer_price, seller_price, seller_qty } = req.body;

  if (buyer_price && buyer_qty) {
    // Insert or update a buyer order
    db.run(
      `INSERT INTO PendingOrderTable (buyer_qty, buyer_price, seller_price, seller_qty) 
       VALUES (?, ?, NULL, NULL)`,
      [buyer_qty, buyer_price],
      function (err) {
        if (err) {
          return res.status(500).json({ message: err.message });
        }
        // Optionally, check if there's a match
        matchOrders();
        res.json({ message: 'Buyer order added to pending orders' });
      }
    );
  } else if (seller_price && seller_qty) {
    // Find a matching buyer order to update
    db.get(
      `SELECT * FROM PendingOrderTable WHERE seller_price IS NULL AND seller_qty IS NULL 
       AND buyer_price >= ? AND buyer_qty >= ?`,
      [seller_price, seller_qty],
      (err, row) => {
        if (err) {
          return res.status(500).json({ message: err.message });
        }
        if (row) {
          // Update the existing row with seller information
          db.run(
            `UPDATE PendingOrderTable 
             SET seller_price = ?, seller_qty = ? 
             WHERE id = ?`,
            [seller_price, seller_qty, row.id],
            function (err) {
              if (err) {
                return res.status(500).json({ message: err.message });
              }
              // Optionally, check if there's a match
              matchOrders();
              res.json({ message: 'Seller order matched and updated in pending orders' });
            }
          );
        } else {
          // If no match, insert a new seller order
          db.run(
            `INSERT INTO PendingOrderTable (buyer_qty, buyer_price, seller_price, seller_qty) 
             VALUES (NULL, NULL, ?, ?)`,
            [seller_price, seller_qty],
            function (err) {
              if (err) {
                return res.status(500).json({ message: err.message });
              }
              res.json({ message: 'Seller order added to pending orders' });
            }
          );
        }
      }
    );
  } else {
    res.status(400).json({ message: 'Invalid order data' });
  }
});

// Get Orders Endpoint
app.get('/orders', (req, res) => {
  db.serialize(() => {
    db.all('SELECT * FROM PendingOrderTable', [], (err, rows) => {
      if (err) {
        return res.status(500).json({ message: err.message });
      }
      db.all('SELECT * FROM CompletedOrderTable', [], (err, completedRows) => {
        if (err) {
          return res.status(500).json({ message: err.message });
        }

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
      });
    });
  });
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
