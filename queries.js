// query.js

const { MongoClient } = require("mongodb");

const uri = "mongodb+srv://njunge995:WKLDboO5KS04jZlA@cluster0.s7dht.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // 1. Find all books in a specific genre
    const adventureBooks = await collection.find({ genre: "Adventure" }).toArray();
    console.log("Books in genre 'Adventure':", adventureBooks);

    // 2. Find books published after 1900
    const booksAfter1900 = await collection.find({ published_year: { $gt: 1900 } }).toArray();
    console.log("Books published after 1900:", booksAfter1900);

    // 3. Find books by a specific author
    const booksByHarperLee = await collection.find({ author: "Harper Lee" }).toArray();
    console.log("Books by Harper Lee:", booksByHarperLee);

    // 4. Find books that are both in stock and published after 2010
    const recentInStockBooks = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log("In-stock books published after 2010:", recentInStockBooks);

    // 5. Projection: Only title, author, and price
    const projectedBooks = await collection.find(
      { genre: "Adventure" },
      { projection: { title: 1, author: 1, price: 1, _id: 0 } }
    ).toArray();
    console.log("Projected fields (title, author, price):", projectedBooks);

    // 6. Sort by price ascending
    const sortByPriceAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log("Books sorted by price (ascending):", sortByPriceAsc);

    // 7. Sort by price descending
    const sortByPriceDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log("Books sorted by price (descending):", sortByPriceDesc);

    // 8. Pagination (page 2, 5 books per page)
    const page = 2;
    const limit = 5;
    const skip = (page - 1) * limit;
    const paginatedBooks = await collection.find().skip(skip).limit(limit).toArray();
    console.log(`Books page ${page}:`, paginatedBooks);

    // 9. Aggregation: Average price by genre
    const avgPriceByGenre = await collection.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" }
        }
      }
    ]).toArray();
    console.log("Average price by genre:", avgPriceByGenre);

    // 10. Aggregation: Author with most books
    const topAuthor = await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("Top author by number of books:", topAuthor);

    // 11. Group books by publication decade
    const booksByDecade = await collection.aggregate([
      {
        $project: {
          decade: {
            $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10]
          }
        }
      },
      {
        $group: {
          _id: "$decade",
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log("Books grouped by decade:", booksByDecade);

    // 12. Create indexes
    await collection.createIndex({ title: 1 });
    await collection.createIndex({ author: 1, published_year: -1 });
    console.log("Indexes created on title and author/published_year");

    // 13. Use explain() to compare performance
    const explainBeforeIndex = await collection.find({ title: "Moby Dick" }).explain("executionStats");
    console.log("Query plan with index (title):", explainBeforeIndex.executionStats);

  } catch (error) {
    console.error("Error running queries:", error);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

runQueries();
