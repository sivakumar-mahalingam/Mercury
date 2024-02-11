[![Java CI with Maven](https://github.com/sivakumar-mahalingam/Mercury/actions/workflows/maven-build-test.yml/badge.svg)](https://github.com/sivakumar-mahalingam/Mercury/actions/workflows/maven-build-test.yml)

# ⚗️Mercury

Mercury is a collection of UDFs for Hive. 
There are two types of Hive UDFs: simple and generic. While simple UDFs are easier to construct, they are less flexible and generally less efficient.

| Category   | Description                                 |
|------------|---------------------------------------------|
| collect    | Deals with collections (Array, Map, Struct) |
| statistics | Deals with statistical operations           |

```
.  
├── ...   
├── collect  
│ ├── ArrayIntersection  
│ ├── ArraySort  
│ ├── ArraySubtract  
│ ├── ArrayUnion  
│ └── ...  
├── statistcs  
│ ├── JaccardSimilarity  
│ └── ...   
└── ...  
```
