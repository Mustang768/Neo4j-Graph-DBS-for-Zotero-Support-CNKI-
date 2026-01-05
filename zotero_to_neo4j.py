import pandas as pd
from neo4j import GraphDatabase
import logging
from typing import Dict, List, Any, Optional
import argparse
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ZoteroToNeo4jImporter:
    """Zotero CSV数据导入Neo4j的处理器"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """
        初始化Neo4j连接
        
        Args:
            neo4j_uri: Neo4j数据库URI，如 "bolt://localhost:7687?database=neo4j"
            neo4j_user: 数据库用户名
            neo4j_password: 数据库密码
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        logger.info(f"已连接到Neo4j: {neo4j_uri}")
        
    def close(self):
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j连接已关闭")
    
    def parse_csv(self, csv_path: str, encoding: str = 'utf-8-sig') -> pd.DataFrame:
        """
        解析Zotero CSV文件
        
        Args:
            csv_path: CSV文件路径
            encoding: 文件编码，默认处理带BOM的UTF-8
            
        Returns:
            清洗后的DataFrame
        """
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_path, encoding=encoding)
            logger.info(f"成功读取CSV文件: {csv_path}, 共 {len(df)} 条记录")
            
            # 数据清洗和重命名列（根据你的CSV文件结构调整）
            column_mapping = {
                'Key': 'zotero_key',
                'Item Type': 'item_type',
                'Publication Year': 'publication_year',
                'Author': 'authors',
                'Title': 'title',
                'Publication Title': 'publication_title',
                'DOI': 'doi',
                'Url': 'url',
                'Abstract Note': 'abstract',
                'Date': 'date',
                'Date Added': 'date_added',
                'Date Modified': 'date_modified',
                'Pages': 'pages',
                'Manual Tags': 'manual_tags',
                'Automatic Tags': 'auto_tags',
                'File Attachments': 'file_attachments',
                'Extra': 'extra_info'  # 这里包含知网下载量等信息
            }
            
            # 应用列名映射
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 处理空值
            for col in ['authors', 'manual_tags', 'auto_tags']:
                if col in df.columns:
                    df[col] = df[col].fillna('')
            
            # 解析额外信息（如知网下载量）
            if 'extra_info' in df.columns:
                df['extra_info_parsed'] = df['extra_info'].apply(self._parse_extra_info)
            
            return df
            
        except Exception as e:
            logger.error(f"读取CSV文件失败: {e}")
            raise
    
    def _parse_extra_info(self, extra_str: str) -> Dict[str, Any]:
        """
        解析Extra字段中的额外信息
        
        Args:
            extra_str: Extra字段的字符串
            
        Returns:
            解析后的字典
        """
        if not isinstance(extra_str, str):
            return {}
        
        info_dict = {}
        try:
            # 示例：解析知网下载量
            if 'download:' in extra_str:
                # 查找下载量信息
                import re
                download_match = re.search(r'download:\s*(\d+)', extra_str)
                if download_match:
                    info_dict['download_count'] = int(download_match.group(1))
            
            # 解析其他可能的信息
            if 'CNKICite:' in extra_str:
                cite_match = re.search(r'CNKICite:\s*(\d+)', extra_str)
                if cite_match:
                    info_dict['cnki_citation'] = int(cite_match.group(1))
                    
            # 解析专业领域
            if 'major:' in extra_str:
                major_match = re.search(r'major:\s*([^\n]+)', extra_str)
                if major_match:
                    info_dict['major_field'] = major_match.group(1).strip()
                    
        except Exception as e:
            logger.warning(f"解析extra_info失败: {e}")
        
        return info_dict
    
    def _split_authors(self, authors_str: str) -> List[str]:
        """
        拆分作者字符串
        
        Args:
            authors_str: 作者字符串，用分号分隔
            
        Returns:
            作者列表
        """
        if not authors_str or not isinstance(authors_str, str):
            return []
        
        # 按分号拆分，清理空格
        authors = [author.strip() for author in authors_str.split(';') if author.strip()]
        return authors
    
    def _split_tags(self, tags_str: str) -> List[str]:
        """
        拆分标签/关键词字符串
        
        Args:
            tags_str: 标签字符串，用分号分隔
            
        Returns:
            标签列表
        """
        if not tags_str or not isinstance(tags_str, str):
            return []
        
        # 按分号拆分，清理空格
        tags = [tag.strip() for tag in tags_str.split(';') if tag.strip()]
        return tags
    
    def import_to_neo4j(self, df: pd.DataFrame, clear_existing: bool = False):
        """
        将DataFrame中的数据导入Neo4j
        
        Args:
            df: 包含文献数据的DataFrame
            clear_existing: 是否清空现有数据
        """
        with self.driver.session() as session:
            # 清空现有数据（可选）
            if clear_existing:
                session.run("MATCH (n) DETACH DELETE n")
                logger.info("已清空现有数据库")
            
            # 批量导入数据
            total_papers = len(df)
            logger.info(f"开始导入 {total_papers} 篇文献...")
            
            for idx, row in df.iterrows():
                try:
                    # 导入单篇文献及其关联数据
                    self._import_paper(session, row)
                    
                    # 进度日志
                    if (idx + 1) % 10 == 0 or (idx + 1) == total_papers:
                        logger.info(f"导入进度: {idx + 1}/{total_papers}")
                        
                except Exception as e:
                    logger.error(f"导入文献失败 (Key: {row.get('zotero_key', 'N/A')}): {e}")
            
            logger.info(f"文献导入完成，共 {total_papers} 篇")
    
    def _import_paper(self, session, paper_row: pd.Series):
        """
        导入单篇文献及其关联节点
        
        Args:
            session: Neo4j会话
            paper_row: 文献数据行
        """
        # 1. 创建或更新文献节点
        paper_properties = {
            'zotero_key': paper_row.get('zotero_key', ''),
            'title': paper_row.get('title', ''),
            'item_type': paper_row.get('item_type', ''),
            'publication_year': paper_row.get('publication_year'),
            'publication_title': paper_row.get('publication_title', ''),
            'doi': paper_row.get('doi', ''),
            'url': paper_row.get('url', ''),
            'abstract': paper_row.get('abstract', '')[:500],  # 限制摘要长度
            'date': paper_row.get('date', ''),
            'pages': paper_row.get('pages', ''),
            'has_pdf': bool(paper_row.get('file_attachments'))
        }
        
        # 添加额外信息
        extra_info = paper_row.get('extra_info_parsed', {})
        if extra_info:
            paper_properties.update(extra_info)
        
        # 创建文献节点
        session.run("""
            MERGE (p:Paper {zotero_key: $zotero_key})
            SET p += $properties,
                p.imported_at = datetime()
            """, 
            zotero_key=paper_properties['zotero_key'],
            properties=paper_properties
        )
        
        # 2. 处理作者节点和关系
        authors_str = paper_row.get('authors', '')
        authors = self._split_authors(authors_str)
        
        for author_name in authors:
            if author_name:
                # 创建作者节点
                session.run("""
                    MERGE (a:Author {name: $name})
                    SET a.last_seen_in = $paper_title
                    """,
                    name=author_name,
                    paper_title=paper_properties['title']
                )
                
                # 创建作者-文献关系
                session.run("""
                    MATCH (p:Paper {zotero_key: $paper_key})
                    MATCH (a:Author {name: $author_name})
                    MERGE (p)-[:AUTHORED_BY]->(a)
                    """,
                    paper_key=paper_properties['zotero_key'],
                    author_name=author_name
                )
        
        # 3. 处理标签/关键词节点和关系
        # 合并手动标签和自动标签
        all_tags = []
        
        manual_tags = self._split_tags(paper_row.get('manual_tags', ''))
        auto_tags = self._split_tags(paper_row.get('auto_tags', ''))
        
        all_tags.extend(manual_tags)
        all_tags.extend(auto_tags)
        
        # 去重
        all_tags = list(set(all_tags))
        
        for tag_name in all_tags:
            if tag_name:
                # 创建关键词节点
                session.run("""
                    MERGE (k:Keyword {name: $name})
                    """,
                    name=tag_name
                )
                
                # 创建文献-关键词关系
                session.run("""
                    MATCH (p:Paper {zotero_key: $paper_key})
                    MATCH (k:Keyword {name: $keyword_name})
                    MERGE (p)-[:HAS_KEYWORD]->(k)
                    """,
                    paper_key=paper_properties['zotero_key'],
                    keyword_name=tag_name
                )
        
        # 4. 扩展框架：添加其他节点类型
        # 这里可以添加更多节点类型的处理逻辑
        self._add_extra_nodes(session, paper_row, paper_properties['zotero_key'])
    
    def _add_extra_nodes(self, session, paper_row: pd.Series, paper_key: str):
        """
        添加额外节点（扩展框架示例）
        
        Args:
            session: Neo4j会话
            paper_row: 文献数据行
            paper_key: 文献的Zotero Key
        """
        # 从多个可能的列名中获取出版社信息，并确保它是有效的字符串
        publisher = None
        possible_publisher_cols = ['Publisher', 'publisher', 'Publisher', 'Place']  # 可能存在的列名

        for col in possible_publisher_cols:
            if col in paper_row and pd.notna(paper_row[col]) and str(paper_row[col]).strip():
                publisher = str(paper_row[col]).strip()
                break

        # 只有找到有效的出版社名称时才创建节点
        if publisher:
            session.run("""
                MERGE (pub:Publisher {name: $name})
                WITH pub
                MATCH (p:Paper {zotero_key: $paper_key})
                MERGE (p)-[:PUBLISHED_BY]->(pub)
                """,
                name=publisher,
                paper_key=paper_key
            )
        else:
            # 可选：记录哪些文献没有出版社信息
            logger.debug(f"文献 {paper_key} 无出版社信息，跳过创建Publisher节点")
            
        
        # 示例2：创建期刊/会议节点
        journal = paper_row.get('Publication Title', '') or paper_row.get('publication_title', '')
        if journal and paper_row.get('item_type') == 'journalArticle':
            session.run("""
                MERGE (j:Journal {name: $name})
                SET j.issn = $issn
                WITH j
                MATCH (p:Paper {zotero_key: $paper_key})
                MERGE (p)-[:PUBLISHED_IN]->(j)
                """,
                name=journal,
                issn=paper_row.get('ISSN', ''),
                paper_key=paper_key
            )
        
        # 示例3：创建主题分类节点（从extra_info解析）
        major_field = paper_row.get('extra_info_parsed', {}).get('major_field')
        if major_field:
            session.run("""
                MERGE (sub:Subject {name: $name})
                WITH sub
                MATCH (p:Paper {zotero_key: $paper_key})
                MERGE (p)-[:BELONGS_TO_SUBJECT]->(sub)
                """,
                name=major_field,
                paper_key=paper_key
            )

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='将Zotero CSV数据导入Neo4j')
    parser.add_argument('--csv', required=True, help='Zotero导出的CSV文件路径')
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI')
    parser.add_argument('--user', default='neo4j', help='Neo4j用户名')
    parser.add_argument('--password', required=True, help='Neo4j密码')
    parser.add_argument('--clear', action='store_true', help='清空现有数据库')
    parser.add_argument('--encoding', default='utf-8-sig', help='CSV文件编码')
    
    args = parser.parse_args()
    
    # 创建导入器实例
    importer = None
    try:
        importer = ZoteroToNeo4jImporter(args.uri, args.user, args.password)
        
        # 解析CSV文件
        df = importer.parse_csv(args.csv, args.encoding)
        
        # 显示数据预览
        logger.info("数据预览:")
        logger.info(f"字段列表: {list(df.columns)}")
        logger.info(f"前几条记录的标题: {df['title'].head(3).tolist() if 'title' in df.columns else '无标题字段'}")
        
        # 导入到Neo4j
        importer.import_to_neo4j(df, args.clear)
        
        logger.info("导入完成！")
        
    except Exception as e:
        logger.error(f"导入过程失败: {e}")
        sys.exit(1)
        
    finally:
        if importer:
            importer.close()

if __name__ == "__main__":
    main()