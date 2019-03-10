import { map, toPairs, size, get, isEmpty } from "lodash";
import { DEFAULT_FUZZINESS, NO_FUZZINESS } from "./constants";
import {
  multiMatch,
  prefixMatch,
  isPhrase,
  phraseMatch,
  boolShould,
  getOptionValue,
  termsQuery,
  termQuery,
  singleFieldTextQueryWithBoost,
  matchesOneBoolQuery,
  matchAllQuery,
  andFilters,
  distanceCalculationScriptField
} from "./utils";

const SortTypes = {
  FieldOrder: {
    applySortToQuery: function(querybuilder, sortParams) {
      const sortField = get(sortParams, "sortField");
      if (isEmpty(sortField)) {
        throw new Error("sortParams.sortField required for FieldOrder sort");
      }
      querybuilder.sorts.push({
        [sortParams.sortField]: get(sortParams, "sortAscending", true)
          ? "asc"
          : "desc"
      });
    }
  },
  Distance: {
    applySortToQuery: function(querybuilder, sortParams) {
      const origin = sortParams.origin;
      if (origin) {
        const lat = origin[1];
        const lon = origin[0];
        querybuilder.sorts.push({
          _geo_distance: {
            geo: {
              lat,
              lon
            },
            order: "asc",
            unit: "m",
            distance_type: "plane"
          }
        });
      }
      return querybuilder;
    }
  },
  Scoring: {
    applySortToQuery: function(querybuilder) {
      // if we're using the score calculated by Elasticsearch, then the returned results are already in the sort
      // order we want
      return querybuilder;
    }
  }
};

/**
 * Creates a new querybuilder object which is used to accumulate the queries, filters, and sorts required for a
 * given query.  Once all the attributes of a query have been applied to the querybuilder object, build(...)
 * must be called to return the actual ElasticSearch query.
 *
 */

export class QueryBuilder {
  queries: Record<string, any>[];
  sorts: Record<string, any>[];
  filters: Record<string, any>[];
  distanceCalc: Record<string, any>;
  minScore: number;
  constructor() {
    this.queries = [];
    this.sorts = [];
    this.filters = [];
  }

  /**
   * Add fuzzy full-text search to the query
   *
   */
  public fuzzyTextQuery(textToSearch: string, fieldsToSearch: string[]) {
    this.queries.push(
      multiMatch(fieldsToSearch, textToSearch, DEFAULT_FUZZINESS)
    );

    return this;
  }

  /**
   * Add full-text search search to the query allowing boosts to be set on each field.
   *   options {Object} - possible options:
   *   fuzzyMatch.enabled - default = true
   *   fuzzyMatch.boostFactor - default = 1
   *   exactMatch.enabled - default = true
   *   exactMatch.boostFactor - default = 2
   *   phraseMatch.enabled - default = true
   *   phraseMatch.boostFactor - default = 4
   *   prefixMatch.enabled - default = false
   *   prefixMatch.boostFactor - default = 1
   */

  public multiFieldTextSearchWithBoost(
    textToSearch: string,
    boostMap: Record<string, any>,
    options
  ) {
    let shouldExpressions = [];

    const fuzzyMatchEnabled = getOptionValue(
      options,
      "fuzzyMatch.enabled",
      true
    );
    if (fuzzyMatchEnabled) {
      const fuzzyMatchBoostFactor = getOptionValue(
        options,
        "fuzzyMatch.boostFactor",
        1
      );
      const shouldSubExpressions = map(toPairs(boostMap), function(kv) {
        const textField = kv[0];
        const boost = kv[1];
        return singleFieldTextQueryWithBoost(
          textField,
          textToSearch,
          boost * fuzzyMatchBoostFactor,
          DEFAULT_FUZZINESS
        );
      });
      shouldExpressions = shouldExpressions.concat(
        boolShould(shouldSubExpressions)
      );
    }

    const exactMatchEnabled = getOptionValue(
      options,
      "exactMatch.enabled",
      true
    );
    if (exactMatchEnabled) {
      const exactMatchBoostFactor = getOptionValue(
        options,
        "exactMatch.boostFactor",
        2
      );
      const shouldExactSubExpressions = map(toPairs(boostMap), function(kv) {
        const textField = kv[0];
        const boost = kv[1];
        return singleFieldTextQueryWithBoost(
          textField,
          textToSearch,
          boost * exactMatchBoostFactor,
          NO_FUZZINESS
        );
      });
      shouldExpressions = shouldExpressions.concat(
        boolShould(shouldExactSubExpressions)
      );
    }

    const phraseMatchEnabled = getOptionValue(
      options,
      "phraseMatch.enabled",
      true
    );
    if (phraseMatchEnabled && isPhrase(textToSearch)) {
      const phraseMatchBoostFactor = getOptionValue(
        options,
        "phraseMatch.boostFactor",
        2
      );
      const shouldPhraseExpressions = map(toPairs(boostMap), function(kv) {
        const textField = kv[0];
        const boost = kv[1];
        return phraseMatch(
          textField,
          textToSearch,
          boost * phraseMatchBoostFactor
        );
      });
      shouldExpressions = shouldExpressions.concat(
        boolShould(shouldPhraseExpressions)
      );
    }

    const prefixMatchEnabled = getOptionValue(
      options,
      "prefixMatch.enabled",
      false
    );
    if (prefixMatchEnabled) {
      const prefixMatchBoostFactor = getOptionValue(
        options,
        "prefixMatch.boostFactor",
        1
      );
      const shouldPrefixExpressions = map(toPairs(boostMap), function(kv) {
        const textField = kv[0];
        const boost = kv[1];
        return prefixMatch(
          textField,
          textToSearch,
          boost * prefixMatchBoostFactor
        );
      });
      shouldExpressions = shouldExpressions.concat(
        boolShould(shouldPrefixExpressions)
      );
    }

    this.queries.push({
      bool: {
        should: shouldExpressions
      }
    });

    return this;
  }

  /**
   * Forces a search for an exact text. Exact searchs are triggered if the user specified a text string single or double
   * quotes (e.g., "the quick brown fox" or 'the brown fox')
   *
   */
  public exactPhraseTextSearchWithBoost(
    textToSearch: string,
    boostMap: Record<string, number>,
    options
  ) {
    let shouldExpressions = [];
    const phraseMatchBoostFactor = getOptionValue(
      options,
      "phraseMatch.boostFactor",
      2
    );
    const shouldPhraseExpressions = map(toPairs(boostMap), function(kv) {
      const textField = kv[0];
      const boost = kv[1];
      return phraseMatch(
        textField,
        textToSearch,
        boost * phraseMatchBoostFactor
      );
    });
    shouldExpressions = shouldExpressions.concat(
      boolShould(shouldPhraseExpressions)
    );
    this.queries.push({
      bool: {
        should: shouldExpressions
      }
    });

    return this;
  }

  /**
   * Exact match filter.
   */
  public filterTerms(docPath: string, value: any) {
    this.filters.push(termsQuery(docPath, value));

    return this;
  }

  /**
   * Exact match filter.
   */
  public filterExact(docPath, value) {
    this.filters.push(termQuery(docPath, value));

    return this;
  }

  /**
   * Add a filter by distance to the query.
   */
  public filterByDistance(docPath, origin, distanceMeters) {
    this.filters.push({
      geo_distance: {
        distance: `${distanceMeters}m`,
        [docPath]: origin
      }
    });

    return this;
  }

  /**
   * Add a range query where the desired value is greater than a threshold.
   */
  public filterGte(docPath, value) {
    this.filters.push({
      range: {
        [docPath]: {
          gte: value
        }
      }
    });

    return this;
  }

  public filterLte(docPath, value) {
    this.filters.push({
      range: {
        [docPath]: {
          lte: value
        }
      }
    });

    return this;
  }

  /**
   * Add a matches one filter to the query
   */
  public filterMatchesOne(docPath, values) {
    this.filters.push(matchesOneBoolQuery(docPath, values));

    return this;
  }

  public filterMust(docPath, value) {
    this.filters.push({
      bool: {
        must: termsQuery(docPath, value)
      }
    });

    return this;
  }

  /**
   * Adds a must_not bool filter to the filter set
   * https://www.elastic.co/guide/en/elasticsearch/guide/current/combining-filters.html#bool-filter
   */
  public filterMustNot(docPath, value) {
    this.filters.push({
      bool: {
        must_not: termQuery(docPath, value)
      }
    });

    return this;
  }

  /**
   *
   * @param sortDescriptor The sortDescriptor contains the sortType (see WPElasticsearch.QueryBuilder.SortTypes) and
   * the sort params structure looks like {sortType: WPElasticsearch.QueryBuilder.SortTypes.xxx, sortParams: { params }
   */
  sortBy(sortDescriptor) {
    const sortType =
      sortDescriptor.sortType && SortTypes[sortDescriptor.sortType];
    if (sortType) {
      sortType.applySortToQuery(this, sortDescriptor.sortParams);
    }

    return this;
  }

  /**
   * Add distance calculation to the query results.
   *
   * @param geoField
   * @param lat
   * @param lon
   * @param distanceField
   */
  addDistanceCalculation(geoField, lat, lon, distanceField) {
    this.distanceCalc = {
      geoField,
      lat,
      lon,
      distanceField
    };

    return this;
  }

  /**
   * Set a min score for search results
   *
   * @param minScore
   * @returns {WPElasticsearch.QueryBuilder}
   */
  setMinScore(minScore) {
    this.minScore = minScore;

    return this;
  }

  /**
   * Build the Elasticsearch query request given the built up parameters/constraints.
   *
   * @returns {Object} The request query.
   */
  build() {
    const query =
      (size(this.queries) > 0 && this.queries[0]) || matchAllQuery();

    // set the filters
    const numFilters = size(this.filters);
    let filter = {};
    switch (numFilters) {
      case 0:
        break;
      case 1:
        filter = this.filters[0];
        break;
      default:
        // more than 1 filter
        filter = andFilters(this.filters);
        break;
    }

    let sort;
    if (!isEmpty(this.sorts)) {
      sort = this.sorts;
    }

    const q = {
      query: {
        bool: {
          must: query,
          filter
        }
      },
      sort
    };

    if (this.distanceCalc) {
      Object.assign(q, distanceCalculationScriptField(this.distanceCalc));
    }

    if (this.minScore) {
      Object.assign(q, {
        min_score: this.minScore
      });
    }

    return q;
  }
}
