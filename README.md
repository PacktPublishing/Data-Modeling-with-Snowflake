# Data Modeling with Snowflake

<a href="https://www.packtpub.com/product/data-modeling-with-snowflake/9781837634453"><img src="https://content.packt.com/B19467/cover_image_small.jpg" alt="Data Modeling with Snowflake" height="256px" align="right"></a>

This is the code repository for [Data Modeling with Snowflake](https://www.packtpub.com/product/data-modeling-with-snowflake/9781837634453), published by Packt.

**A practical guide to accelerating Snowflake development using universal data modeling techniques**

## What is this book about?
The Snowflake Data Cloud is one of the fastest-growing platforms for data warehousing and application workloads. Snowflake's scalable, cloud-native architecture and expansive set of features and objects enables you to deliver data solutions quicker than ever before.
Yet, we must ensure that these solutions are developed using recommended design patterns and accompanied by documentation that’s easily accessible to everyone in the organization.
This book will help you get familiar with simple and practical data modeling frameworks that accelerate agile design and evolve with the project from concept to code. These universal principles have helped guide database design for decades, and this book pairs them with unique Snowflake-native objects and examples like never before – giving you a two-for-one crash course in theory as well as direct application.
By the end of this Snowflake book, you’ll have learned how to leverage Snowflake’s innovative features, such as time travel, zero-copy cloning, and change-data-capture, to create cost-effective, efficient designs through time-tested modeling principles that are easily digestible when coupled with real-world examples.

This book covers the following exciting features: 
* Discover the time-saving features and applications of data modeling
* Explore Snowflake’s cloud-native architecture and features
* Understand and apply modeling concepts, techniques, and language using Snowflake objects
* Master modeling concepts such as normalization and slowly changing dimensions
* Get comfortable reading and transforming semi-structured data
* Work directly with pre-built recipes and examples
* Apply modeling frameworks from Star to Data Vault

If you feel this book is for you, get your [copy](https://www.amazon.com/Data-Modeling-Snowflake-accelerating-development/dp/1837634459) today!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" alt="https://www.packtpub.com/" border="5" /></a>

## Instructions and Navigations
All of the code is organized into folders.

The code will look like the following:
```
-- Query the change tracking metadata to observe
-- only inserts from the timestamp till now
select * from myTable
changes(information => append_only)
at(timestamp => $cDts);
```

**Following is what you need for this book:**
This book is for developers working with SQL who are looking to build a strong foundation in modeling best practices and gain an understanding of where they can be effectively applied to save time and effort. Whether you’re an ace in SQL logic or starting out in database design, this book will equip you with the practical foundations of data modeling to guide you on your data journey with Snowflake. Developers who’ve recently discovered Snowflake will be able to uncover its core features and learn to incorporate them into universal modeling frameworks.	

With the following software and hardware list you can run all code files present in the book (Chapter 1-18).

### Software and Hardware List

| Chapter  | Software required                                                                    | OS required                        |
| -------- | -------------------------------------------------------------------------------------| -----------------------------------|
|  1-18		  |   		Snowflake Data Cloud   | Windows, Mac OS X, and Linux (Any) |
| 1-18         |   			SQL																		  |        Windows, Mac OS X, and Linux (Any)                             |


### Related products <Other books you may enjoy>
*Data Modeling with Tableau [[Packt]](https://www.packtpub.com/product/data-modeling-with-tableau/9781803248028) [[Amazon]](https://www.amazon.in/Data-Modeling-Tableau-practical-building/dp/1803248025)

*SQL Query Design Patterns and Best Practices [[Packt]](https://www.packtpub.com/product/sql-query-design-patterns-and-best-practices/9781837633289) [[Amazon]](https://www.amazon.com/Query-Design-Patterns-Best-Practices/dp/1837633282)

## Get to Know the Author(s)
**Serge Gershkovich** is a data architect with an extensive background in designing and maintaining enterprise-scale data warehouse platforms and reporting solutions.
After a decade of working with the SAP ecosystem, Serge discovered Snowflake and never looked back. His passion for the Data Cloud has led to his creating educational content and being named a Snowflake Data Superhero. Serge is currently engaged as a developer advocate at SqlDBM, an online database modeling tool.
Serge holds a B.Sc. degree in Information Systems from SUNY Stony Brook. Originally from Ukraine, he was raised in New York and has lived in London and Madrid.
He currently resides in Palma de Mallorca with his wife and son.


