# HadoopInvertedIndex

- Implement a inverted index function on Hadoop.

[DataSet](https://drive.google.com/file/d/1zqcp9igDWmzo3r7_zFOF0t4jhOxQibPb/view?usp=sharing)

[Deployed on Google Cloud](https://www.maolintu.com/2018/01/08/running-hadoop-program-on-google-cloud-platform/)

- Output format

`Word BookName: counts`

Example:
`a	Henry James___Four Meetings: 438	 Henry James___Georgina's Reasons: 1708`

means the word `a` occurs 438 times in book Henry James___Four Meetings, 1708 times in book Henry James___Georgina's Reasons

Local test method:
```
<input_path>
<output_path>
-map <docID2Name_path>
```

