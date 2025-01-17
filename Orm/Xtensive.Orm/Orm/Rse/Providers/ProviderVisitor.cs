// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexis Kochetov
// Created:    2009.02.10

using System;


namespace Xtensive.Orm.Rse.Providers
{
  /// <summary>
  /// Abstract <see cref="CompilableProvider"/> visitor class.
  /// </summary>
  [Serializable]
  public abstract class ProviderVisitor<TResult> where TResult : Provider
  {
    /// <summary>
    /// Visits the specified <paramref name="cp"/>.
    /// </summary>
    /// <param name="cp">The <see cref="CompilableProvider"/> to visit.</param>
    /// <returns>Visit result.</returns>
    protected virtual TResult Visit(CompilableProvider cp) =>
      cp == null
        ? null
        : cp.Type switch {
          ProviderType.Index => VisitIndex((IndexProvider) cp),
          ProviderType.Store => VisitStore((StoreProvider) cp),
          ProviderType.Aggregate => VisitAggregate((AggregateProvider) cp),
          ProviderType.Alias => VisitAlias((AliasProvider) cp),
          ProviderType.Calculate => VisitCalculate((CalculateProvider) cp),
          ProviderType.Distinct => VisitDistinct((DistinctProvider) cp),
          ProviderType.Filter => VisitFilter((FilterProvider) cp),
          ProviderType.Join => VisitJoin((JoinProvider) cp),
          ProviderType.Sort => VisitSort((SortProvider) cp),
          ProviderType.Raw => VisitRaw((RawProvider) cp),
          ProviderType.Seek => VisitSeek((SeekProvider) cp),
          ProviderType.Select => VisitSelect((SelectProvider) cp),
          ProviderType.Tag => VisitTag((TagProvider) cp),
          ProviderType.Skip => VisitSkip((SkipProvider) cp),
          ProviderType.Take => VisitTake((TakeProvider) cp),
          ProviderType.Paging => VisitPaging((PagingProvider)cp),
          ProviderType.RowNumber => VisitRowNumber((RowNumberProvider)cp),
          ProviderType.Apply => VisitApply((ApplyProvider)cp),
          ProviderType.Existence => VisitExistence((ExistenceProvider)cp),
          ProviderType.PredicateJoin => VisitPredicateJoin((PredicateJoinProvider)cp),
          ProviderType.Intersect => VisitIntersect((IntersectProvider)cp),
          ProviderType.Except => VisitExcept((ExceptProvider)cp),
          ProviderType.Concat => VisitConcat((ConcatProvider)cp),
          ProviderType.Union => VisitUnion((UnionProvider)cp),
          ProviderType.Lock => VisitLock((LockProvider) cp),
          ProviderType.Include => VisitInclude((IncludeProvider) cp),
          ProviderType.FreeText => VisitFreeText((FreeTextProvider) cp),
          ProviderType.ContainsTable => VisitContainsTable((ContainsTableProvider) cp),
          ProviderType.Void => throw new NotSupportedException(Strings.ExProcessingOfVoidProviderIsNotSupported),
          _ => throw new ArgumentOutOfRangeException()
        };

    /// <summary>
    /// Visits <see cref="PredicateJoinProvider"/>.
    /// </summary>
    /// <param name="provider">Predicate join provider.</param>
    protected abstract TResult VisitPredicateJoin(PredicateJoinProvider provider);

    /// <summary>
    /// Visits <see cref="ExistenceProvider"/>.
    /// </summary>
    /// <param name="provider">Existence provider.</param>
    protected abstract TResult VisitExistence(ExistenceProvider provider);

    /// <summary>
    /// Visits <see cref="ApplyProvider"/>.
    /// </summary>
    /// <param name="provider">Apply provider.</param>
    protected abstract TResult VisitApply(ApplyProvider provider);

    /// <summary>
    /// Visits <see cref="RowNumberProvider"/>.
    /// </summary>
    /// <param name="provider">Row number provider.</param>
    protected abstract TResult VisitRowNumber(RowNumberProvider provider);

    /// <summary>
    /// Visits <see cref="TakeProvider"/>.
    /// </summary>
    /// <param name="provider">Take provider.</param>
    protected abstract TResult VisitTake(TakeProvider provider);

    /// <summary>
    /// Visits <see cref="SkipProvider"/>.
    /// </summary>
    /// <param name="provider">Skip provider.</param>
    protected abstract TResult VisitSkip(SkipProvider provider);

    /// <summary>
    /// Visits <see cref="PagingProvider"/>.
    /// </summary>
    /// <param name="provider">Paging provider.</param>
    protected abstract TResult VisitPaging(PagingProvider provider);

    /// <summary>
    /// Visits <see cref="SelectProvider"/>.
    /// </summary>
    /// <param name="provider">Select provider.</param>
    protected abstract TResult VisitSelect(SelectProvider provider);

    /// <summary>
    /// Visits <see cref="TagProvider"/>.
    /// </summary>
    /// <param name="provider">Tag provider.</param>
    protected abstract TResult VisitTag(TagProvider provider);

    /// <summary>
    /// Visits <see cref="SeekProvider"/>.
    /// </summary>
    /// <param name="provider">Seek provider.</param>
    protected abstract TResult VisitSeek(SeekProvider provider);

    /// <summary>
    /// Visits <see cref="RawProvider"/>.
    /// </summary>
    /// <param name="provider">Raw provider.</param>
    protected abstract TResult VisitRaw(RawProvider provider);

    /// <summary>
    /// Visits <see cref="SortProvider"/>.
    /// </summary>
    /// <param name="provider">Sort provider.</param>
    protected abstract TResult VisitSort(SortProvider provider);

    /// <summary>
    /// Visits <see cref="JoinProvider"/>.
    /// </summary>
    /// <param name="provider">Join provider.</param>
    protected abstract TResult VisitJoin(JoinProvider provider);

    /// <summary>
    /// Visits <see cref="FilterProvider"/>.
    /// </summary>
    /// <param name="provider">Filter provider.</param>
    protected abstract TResult VisitFilter(FilterProvider provider);

    /// <summary>
    /// Visits <see cref="DistinctProvider"/>.
    /// </summary>
    /// <param name="provider">Distinct provider.</param>
    protected abstract TResult VisitDistinct(DistinctProvider provider);

    /// <summary>
    /// Visits <see cref="CalculateProvider"/>.
    /// </summary>
    /// <param name="provider">Calculate provider.</param>
    protected abstract TResult VisitCalculate(CalculateProvider provider);

    /// <summary>
    /// Visits <see cref="AliasProvider"/>.
    /// </summary>
    /// <param name="provider">Alias provider.</param>
    protected abstract TResult VisitAlias(AliasProvider provider);

    /// <summary>
    /// Visits <see cref="AggregateProvider"/>.
    /// </summary>
    /// <param name="provider">Aggregate provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitAggregate(AggregateProvider provider);

    /// <summary>
    /// Visits <see cref="StoreProvider"/>.
    /// </summary>
    /// <param name="provider">Store provider.</param>
    protected abstract TResult VisitStore(StoreProvider provider);

    /// <summary>
    /// Visits <see cref="IndexProvider"/>.
    /// </summary>
    /// <param name="provider">Index provider.</param>
    protected abstract TResult VisitIndex(IndexProvider provider);

    /// <summary>
    /// Visits the <see cref="IntersectProvider"/>.
    /// </summary>
    /// <param name="provider">Intersect provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitIntersect(IntersectProvider provider);

    /// <summary>
    /// Visits the <see cref="ExceptProvider"/>.
    /// </summary>
    /// <param name="provider">Except provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitExcept(ExceptProvider provider);

    /// <summary>
    /// Visits the <see cref="ConcatProvider"/>.
    /// </summary>
    /// <param name="provider">Concat provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitConcat(ConcatProvider provider);

    /// <summary>
    /// Visits the <see cref="UnionProvider"/>.
    /// </summary>
    /// <param name="provider">Union provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitUnion(UnionProvider provider);

    /// <summary>
    /// Visits the <see cref="LockProvider"/>.
    /// </summary>
    /// <param name="provider">Lock provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitLock(LockProvider provider);

    /// <summary>
    /// Visits the <see cref="IncludeProvider"/>.
    /// </summary>
    /// <param name="provider">Include provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitInclude(IncludeProvider provider);

    /// <summary>
    /// Visits the <see cref="FreeTextProvider"/>.
    /// </summary>
    /// <param name="provider">FreeText provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitFreeText(FreeTextProvider provider);

    /// <summary>
    /// Visits the <see cref="FreeTextProvider"/>.
    /// </summary>
    /// <param name="provider">SearchCondition provider.</param>
    /// <returns></returns>
    protected abstract TResult VisitContainsTable(ContainsTableProvider provider);
  }
}
