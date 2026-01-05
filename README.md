# Script_for_Zotero-Neo4j_Support_CNKI

##介绍

-**EN**:This is a Zotero-compatible script designed to export categorized CSV files into Neo4j. 
It enables the analysis of logical and statistical relationships among papers, keywords, authors, and other information, facilitating the rapid clarification of research directions or enhancing literature management efficiency.
-**CN**：这是一个适配zotero的脚本，用于将其分类导出的CSV文件转存入Neo4j中以便我们分析论文、关键词、作者等信息的逻辑关系与统计关系，有利于快速理清论文写作方向或是提高文献管理效率。
并且该脚本针对CNKI论文的数据格式进行了优化。

## 环境要求
- Python 3.11
-Zotero
-Neo4j Desktop(推荐) or Neo4j
-Zotero 茉莉花插件(Jasminum)
-该脚本的数据清洗主要针对知网（CNKI）论文，元数据抓取依赖茉莉花插件，
对于外文文献和非知网文献可能会由于空值引发Neo4j报错，目前对于外文文献的适配正在开发ing...