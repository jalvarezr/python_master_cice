from scraping.aggregations.ResultsMerger import ResultsMerger

merger = ResultsMerger()
results = merger.merge()
merger.save(results)

