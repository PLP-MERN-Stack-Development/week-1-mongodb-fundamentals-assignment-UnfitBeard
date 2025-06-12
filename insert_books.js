// insert_books.js - Script to populate MongoDB with sample book data

// Import MongoDB client
const { MongoClient } = require("mongodb");

// Connection URI (replace with your MongoDB connection string if using Atlas)
const uri = "mongodb+srv://njunge995:WKLDboO5KS04jZlA@cluster0.s7dht.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";

// Database and collection names
const dbName = "plp_bookstore";
const collectionName = "books";

// Sample book data
const books = [
  {
    title: "To Kill a Mockingbird",
    author: "Harper Lee",
    genre: "Fiction",
    published_year: 1960,
    price: 12.99,
    in_stock: true,
    pages: 336,
    publisher: "J. B. Lippincott & Co.",
  },
  {
    title: "1984",
    author: "George Orwell",
    genre: "Dystopian",
    published_year: 1949,
    price: 10.99,
    in_stock: true,
    pages: 328,
    publisher: "Secker & Warburg",
  },
  {
    title: "The Great Gatsby",
    author: "F. Scott Fitzgerald",
    genre: "Fiction",
    published_year: 1925,
    price: 9.99,
    in_stock: true,
    pages: 180,
    publisher: "Charles Scribner's Sons",
  },
  {
    title: "Brave New World",
    author: "Aldous Huxley",
    genre: "Dystopian",
    published_year: 1932,
    price: 11.5,
    in_stock: false,
    pages: 311,
    publisher: "Chatto & Windus",
  },
  {
    title: "The Hobbit",
    author: "J.R.R. Tolkien",
    genre: "Fantasy",
    published_year: 1937,
    price: 14.99,
    in_stock: true,
    pages: 310,
    publisher: "George Allen & Unwin",
  },
  {
    title: "The Catcher in the Rye",
    author: "J.D. Salinger",
    genre: "Fiction",
    published_year: 1951,
    price: 8.99,
    in_stock: true,
    pages: 224,
    publisher: "Little, Brown and Company",
  },
  {
    title: "Pride and Prejudice",
    author: "Jane Austen",
    genre: "Romance",
    published_year: 1813,
    price: 7.99,
    in_stock: true,
    pages: 432,
    publisher: "T. Egerton, Whitehall",
  },
  {
    title: "The Lord of the Rings",
    author: "J.R.R. Tolkien",
    genre: "Fantasy",
    published_year: 1954,
    price: 19.99,
    in_stock: true,
    pages: 1178,
    publisher: "Allen & Unwin",
  },
  {
    title: "Animal Farm",
    author: "George Orwell",
    genre: "Political Satire",
    published_year: 1945,
    price: 8.5,
    in_stock: false,
    pages: 112,
    publisher: "Secker & Warburg",
  },
  {
    title: "The Alchemist",
    author: "Paulo Coelho",
    genre: "Fiction",
    published_year: 1988,
    price: 10.99,
    in_stock: true,
    pages: 197,
    publisher: "HarperOne",
  },
  {
    title: "Moby Dick",
    author: "Herman Melville",
    genre: "Adventure",
    published_year: 1851,
    price: 12.5,
    in_stock: false,
    pages: 635,
    publisher: "Harper & Brothers",
  },
  {
    title: "Wuthering Heights",
    author: "Emily Brontë",
    genre: "Gothic Fiction",
    published_year: 1847,
    price: 9.99,
    in_stock: true,
    pages: 342,
    publisher: "Thomas Cautley Newby",
  },
];

// Function to insert books into MongoDB
async function insertBooks() {
  const client = new MongoClient(uri);

  try {
    // Connect to the MongoDB server
    await client.connect();
    console.log("Connected to MongoDB server");

    // Get database and collection
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Check if collection already has documents
    const count = await collection.countDocuments();
    if (count > 0) {
      console.log(
        `Collection already contains ${count} documents. Dropping collection...`
      );
      await collection.drop();
      console.log("Collection dropped successfully");
    }

    // Insert the books
    const result = await collection.insertMany(books);
    console.log(
      `${result.insertedCount} books were successfully inserted into the database`
    );

    // Display the inserted books
    console.log("\nInserted books:");
    const insertedBooks = await collection.find({}).toArray();
    insertedBooks.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        })`
      );
    });

    // Find all books in a specific Genre
    console.log("Genre is equal to Adventure");
    const genreIsAdventure = await collection
      .find({ genre: "Adventure" })
      .toArray();
    genreIsAdventure.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        })`
      );
    });

    // Find books published after a certain Year
    console.log("Published after 1900");
    const yearAfter1900 = await collection
      .find({ published_year: { $gt: 1900 } })
      .toArray();
    yearAfter1900.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        })`
      );
    });

    // Find all books by a specific author
    console.log("Author is Harper Lee");
    const booksByHarperLee = await collection
      .find({ author: "Harper Lee" })
      .toArray();
    booksByHarperLee.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        })`
      );
    });

    // Update Price of a specific book
    console.log("Update Price of 1984 by George Owell");
    const updateResult = await collection.updateOne(
      { genre: "Adventure" },
      { $set: { price: 20.99 } }
    );
    if (updateResult.modifiedCount === 1) {
      console.log("Price updated successfully.");
    } else {
      console.log("Price update failed or no document matched.");
    }

    // Fetch and display updated document
    const updatedBook = await collection.find({ title: "Moby Dick" }).toArray();
    updatedBook.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        }) - $${book.price}`
      );
    });

    // Delete a book by its title
    const deletedBook = await collection.deleteOne({
      title: "To Kill a Mockingbird",
    });
    if (deletedBook.deletedCount === 0) {
      console.log("Error no deletion occured");
    } else {
      console.log("Sucessfully deleted the book");
    }

    // TASK 3
    //1-query to find books that are both in stock and published after 2010
    console.log(`books that are both in stock and published after 2010`);
    const inStockAndPublishedAfter2010 = await collection
      .aggregate([
        { $match: { in_stock: true, published_year: { $gt: 2010 } } },
      ])
      .toArray();
    inStockAndPublishedAfter2010.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        }) - $${book.price}`
      );
    });

    //2-projection to return only the title, author, and price fields in your queries
    console.log(
      `projection to return only the title, author, and price fields in your queries`
    );
    const projection = await collection
      .find(
        { genre: "Adventure" },
        {
          projection: {
            title: 1,
            author: 1,
            price: 1,
            _id: 1, // exclude _id; use 1 if you want to include it
          },
        }
      )
      .toArray();
    projection.forEach((book) => {
      console.log(
        `${book._id}. "${book.title}" by ${book.author} - $${book.price}`
      );
    });

    // Implement sorting to display books by price (both ascending and descending)
    console.log(`Sort ascending by price`);
    const sortAscending = await collection
      .aggregate([{ $sort: { price: -1 } }])
      .toArray();
    sortAscending.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        }) - $${book.price}`
      );
    });

    console.log(`sort Descending by Price`);
    const sortDescending = await collection
      .aggregate([{ $sort: { price: 1 } }])
      .toArray();
    sortDescending.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} (${
          book.published_year
        }) - $${book.price}`
      );
    });

    // Use the `limit` and `skip` methods to implement pagination (5 books per page)
    const page = 2; // You can change this to any page number
    const limit = 5;
    const skip = (page - 1) * limit;
    console.log(`Limit and skip for pagination`);
    const paginatedBooks = await collection
      .find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } })
      .skip(skip)
      .limit(limit)
      .toArray();

    paginatedBooks.forEach((book, index) => {
      console.log(
        `${index + 1}. "${book.title}" by ${book.author} - $${book.price}`
      );
    });

    // - Create an aggregation pipeline to calculate the average price of books by genre
    console.log(
      `aggregation pipeline to calculate the average price of books by genre`
    );
    const avgGenre = await collection
      .aggregate([
        {
          $group: {
            _id: "$genre",
            averagePrice: { $avg: "$price" },
          },
        },
      ])
      .toArray();

    avgGenre.forEach((genreStat, index) => {
      console.log(
        `${index + 1}. Genre: ${
          genreStat._id
        } - Average Price: $${genreStat.averagePrice.toFixed(2)}`
      );
    });

    // - Create an aggregation pipeline to find the author with the most books in the collection
    console.log(
      `aggregation pipeline to find the author with the most books in the collection`
    );
    const topAuthor = await collection
      .aggregate([
        {
          $group: {
            _id: "$author",
            bookCount: { $sum: 1 },
          },
        },
        {
          $sort: { bookCount: -1 },
        },
        {
          $limit: 1,
        },
      ])
      .toArray();

    console.log(
      `Top Author: ${topAuthor[0]._id} with ${topAuthor[0].bookCount} books.`
    );

    // - Implement a pipeline that groups books by publication decade and counts them
    console.log(
      `pipeline that groups books by publication decade and counts them`
    );
    const booksByDecade = await collection
      .aggregate([
        {
          $project: {
            decade: {
              $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10],
            },
          },
        },
        {
          $group: {
            _id: "$decade",
            count: { $sum: 1 },
          },
        },
        {
          $sort: { _id: 1 }, // Optional: Sort by decade
        },
      ])
      .toArray();

      //Task 5: Indexing
// - Create an index on the `title` field for faster searches
await collection.createIndex({ title: 1})

// - Create a compound index on `author` and `published_year`
await collection.createIndex({author:1, published_year: -1})
// - Use the `explain()` method to demonstrate the performance improvement with your indexes
const noIndexExplain = await collection.find({ title: "Moby Dick" }).explain("executionStats");
console.log("Without index:\n", noIndexExplain.executionStats);

// after indexing
const indexExplain = await collection.find({title: "Moby Dick"}).explain("executionStats")
console.log("With index:\n", indexExplain.executionStats);

    booksByDecade.forEach((entry) => {
      console.log(`Decade: ${entry._id}s - ${entry.count} books`);
    });
  } catch (err) {
    console.error("Error occurred:", err);
  } finally {
    // Close the connection
    await client.close();
    console.log("Connection closed");
  }
}

// Run the function
insertBooks().catch(console.error);

/*
 * Example MongoDB queries you can try after running this script:
 *
 * 1. Find all books:
 *    db.books.find()
 *
 * 2. Find books by a specific author:
 *    db.books.find({ author: "George Orwell" })
 *
 * 3. Find books published after 1950:
 *    db.books.find({ published_year: { $gt: 1950 } })
 *
 * 4. Find books in a specific genre:
 *    db.books.find({ genre: "Fiction" })
 *
 * 5. Find in-stock books:
 *    db.books.find({ in_stock: true })
 */
