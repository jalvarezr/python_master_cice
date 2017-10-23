from scraping.aggregations.TeamsNormalizer import TeamsNormalizer

normalizer = TeamsNormalizer()
result = normalizer.normalize()
normalizer.save_csv(result)
