# GEOSHIELD System

## The Challenge
In an era where security threats, antisemitism, and natural disasters are increasingly complex and prevalent, traditional methods often fall short in managing the vast and diverse array of data sources. This leads to delays and inaccuracies in responses. The pressing need is for a system capable of handling large volumes of varied data and providing real-time, precise insights.

## Our Goal
To address this challenge, our objective was to develop a robust platform that excels in deep analysis rather than superficial processing. Our aim was to create a system that meticulously manages complex data loads and diverse structures, delivering actionable information efficiently.

## The Solution
GEOSHIELD is designed as a three-stage system to tackle these challenges effectively:

1. **Data Collection**: Data is gathered from multiple sources using APIs, pulling information from various platforms with different data structures.

2. **Data Classification**: Advanced Natural Language Processing (NLP) techniques are employed to categorize and interpret the collected data. During this phase, classified news articles are also sent to an AI tool (AI21) for location extraction, enabling in-depth text analysis and context understanding.

3. **Correlation and Analysis**: Machine learning algorithms and AI models are used to analyze and correlate the data in real-time, providing high-precision updates.

## Our Contribution
We played a pivotal role in developing GeoShield's core pipeline, ensuring each stage could handle large volumes of data from various sources with different structures. Our focus was on deepening the analysis at each stage, particularly through the development and integration of a pre-trained machine learning model trained on approximately 60,000 classified news articles. This approach allowed GeoShield to deliver highly accurate and timely insights, distinguishing it from other platforms that may offer broader coverage but lack depth in analysis.

## Code Structure
The code in this repository is organized into folders, where each folder represents a specific Lambda function along with any sample configuration files, if they exist.

### Lambda Functions and Modules


1. **Data Collection Module**: Responsible for collecting raw data from various sources using the following Lambda functions:
   - `Data_Collection`: General collection of data from all connected sources.
   - `GDELT_Data_Collection`: Focuses on data gathering from GDELT using its API.
   - `TELGRAM_Data_Collection`: Extracts data from Telegram using string sessions.
   - `Summarize_Articles`: Processes and summarizes articles collected from various sources.
   - `Set_Config`: Collects custom information based on user-defined parameters.

2. **Data Pipeline Module**: Processes and enriches the collected data while filtering out irrelevant information through:
   - `Data_Classification`: Classifies collected information into specific categories (e.g., security, antisemitism, natural disasters).
   - `Data_Extract_Location`: Extracts geographical data using AI tools.
   - `Data_Extract_Events`: Identifies key events from the collected data.
   - `Data_Correlation`: Finds relationships between different data points to generate actionable insights.

3. **Data Filter Module**: Adjusts the output based on user requests:
   - `Data_Statistics`: Generates statistical insights for graphical representation.
   - `Get_Json`: Filters and provides specific information based on user queries.




