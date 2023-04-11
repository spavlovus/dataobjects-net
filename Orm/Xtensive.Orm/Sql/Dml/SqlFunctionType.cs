// Copyright (C) 2007-2023 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.

using System;

namespace Xtensive.Sql.Dml
{
  [Serializable]
  public enum SqlFunctionType
  {
    Concat,
    CurrentDate,
    CurrentDateTimeOffset,
    CurrentTime,
    CurrentTimeStamp,
    Lower,
    CharLength,
    BinaryLength,
    Position,
    Replace,
    Substring,
    Upper,
    UserDefined,
    CurrentUser,
    SessionUser,
    SystemUser,
    User,
    NullIf,
    Coalesce,
    LastAutoGeneratedId,
    PadLeft,
    PadRight,

    // mathematical functions
    Abs,
    Acos,
    Asin,
    Atan,
    Atan2,
    Ceiling,
    Cos,
    Cot,
    Degrees,
    Exp,
    Floor,
    Log,
    Log10,
    Pi,
    Power,
    Radians,
    Rand,
    Round,
    Truncate,
    Sign,
    Sin,
    Sqrt,
    Square,
    Tan,

    // date time / interval functions
    // not ansi sql but our cross-server solution
#if NET6_0_OR_GREATER
    DateConstruct,
    DateAddYears,
    DateAddMonths,
    DateAddDays,
    DateToString,
    DateToDateTime,
    DateToDateTimeOffset,
    TimeConstruct,
    TimeAddHours,
    TimeAddMinutes,
    TimeToString,
    TimeToDateTime,
    TimeToDateTimeOffset,
    TimeToNanoseconds,
#endif

    DateTimeConstruct,
    DateTimeAddYears,
    DateTimeAddMonths,
    DateOnlyAddDays,
    TimeOnlyAddHours,
    TimeOnlyAddMinutes,
    DateTimeTruncate,
    DateTimeToStringIso,
#if NET6_0_OR_GREATER
    DateTimeToTime,
    DateTimeToDate,
#endif
    IntervalConstruct,
    IntervalToMilliseconds,
    IntervalToNanoseconds,
    IntervalAbs,
    IntervalNegate,

    // DateTimeOffset / interval functions
    DateTimeOffsetConstruct,
    DateTimeOffsetAddYears,
    DateTimeOffsetAddMonths,
    DateTimeOffsetTimeOfDay,
    DateTimeOffsetToLocalTime,
    DateTimeOffsetToUtcTime,
    DateTimeToDateTimeOffset,
    DateTimeOffsetToDateTime,
#if NET6_0_OR_GREATER
    DateTimeOffsetToTime,
    DateTimeOffsetToDate,
    DateOnlyConstruct,
    TimeOnlyConstruct,
#endif

    // .NET like rounding functions

    RoundDecimalToEven,
    RoundDecimalToZero,
    RoundDoubleToEven,
    RoundDoubleToZero,
    //!!! max value is used for array size
  }
}
