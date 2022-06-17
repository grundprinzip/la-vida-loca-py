import base64
from calendar import c
from typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)

import pyspark.sql.connect.proto.spark_connect_pb2 as proto
from pyspark.sql.connect.column import (
    ColumnOrString,
    ColumnRef,
    Expression,
    ExpressionOrString,
    SortOrder,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import RemoteSparkSession


class LogicalPlan(object):

    INDENT = 2

    def __init__(self, child: Optional["LogicalPlan"]) -> None:
        self._child = child

    def unresolved_attr(self, *colNames: str) -> proto.Expression:
        """Creates an unresolved attribute from a column name."""
        exp = proto.Expression()
        exp.unresolved_attribute.parts.extend(list(colNames))
        return exp

    def to_attr_or_expression(
        self, col: ColumnOrString, session: "RemoteSparkSession"
    ) -> proto.Expression:
        """Returns either an instance of an unresolved attribute or the serialized
        expression value of the column."""
        if type(col) is str:
            return self.unresolved_attr(cast(str, col))
        else:
            return cast(ColumnRef, col).to_plan(session)

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        ...

    def _verify(self, session: "RemoteSparkSession") -> bool:
        """This method is used to verify that the current logical plan
        can be serialized to Proto and back and afterwards is identical."""
        plan = proto.Plan()
        plan.root.CopyFrom(self.plan(session))

        serialized_plan = plan.SerializeToString()
        test_plan = proto.Plan()
        test_plan.ParseFromString(serialized_plan)

        return test_plan == plan

    # TODO(martin.grund) explain , schema
    def collect(self, session: "RemoteSparkSession", debug: bool = False):
        plan = proto.Plan()
        plan.root.CopyFrom(self.plan(session))

        if debug:
            print(plan)

        return plan

    def _i(self, indent) -> str:
        return " " * indent

    def print(self, indent=0) -> str:
        ...

    def _repr_html_(self):
        ...


class Read(LogicalPlan):
    def __init__(self, table_name: str) -> None:
        super().__init__(None)
        self.table_name = table_name

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        plan = proto.Relation()
        plan.read.named_table.parts.append(self.table_name)
        return plan

    def print(self, indent=0) -> str:
        return f"{self._i(indent)}<Read table_name={self.table_name}>\n"

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Read</b><br />
                table name: {self.table_name}
            </li>
        </ul>
        """


class Project(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], *columns: ExpressionOrString
    ) -> None:
        super().__init__(child)
        self._raw_columns = list(columns)
        self.alias = None

    def withAlias(self, alias) -> LogicalPlan:
        self.alias = alias
        return self

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        proj_exprs = [
            c.to_plan(session)
            if isinstance(c, Expression)
            else self.unresolved_attr(*cast(str, c).split("."))
            for c in self._raw_columns
        ]  # [self.unresolved_attr(*x) for x in self.columns]
        common = proto.RelationCommon()
        if not self.alias is None:
            common.alias = self.alias

        plan = proto.Relation()
        plan.project.input.CopyFrom(self._child.plan(session))
        plan.project.expressions.extend(proj_exprs)
        plan.common.CopyFrom(common)
        return plan

    def print(self, indent=0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{self._i(indent)}<Project cols={self._raw_columns}>\n{c_buf}"

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Project</b><br />
                Columns: {",".join([str(c) for c in self._raw_columns])}
                {self._child._repr_html_()}
            </li>
        </uL>
        """


class Filter(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], filter: Expression) -> None:
        super().__init__(child)
        self.filter = filter

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.filter.input.CopyFrom(self._child.plan(session))
        plan.filter.condition.CopyFrom(self.filter.to_plan(session))
        return plan

    def print(self, indent=0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{self._i(indent)}<Filter filter={self.filter}>\n{c_buf}"

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Filter</b><br />
                Condition: {self.filter}
                {self._child._repr_html_()}
            </li>
        </uL>
        """


class Limit(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], limit: int, offset: int = 0
    ) -> None:
        super().__init__(child)
        self.limit = limit
        self.offset = offset

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.fetch.input.CopyFrom(self._child.plan(session))
        plan.fetch.limit = self.limit
        return plan

    def print(self, indent=0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return (
            f"{self._i(indent)}<Limit limit={self.limit} offset={self.offset}>\n{c_buf}"
        )

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Limit</b><br />
                Limit: {self.limit} <br />
                Offset: {self.offset} <br />
                {self._child._repr_html_()}
            </li>
        </uL>
        """


class Sort(LogicalPlan):
    def __init__(
        self, child: Optional["LogicalPlan"], *columns: Union[SortOrder, ColumnRef, str]
    ) -> None:
        super().__init__(child)
        self.columns = list(columns)

    def col_to_sort_field(
        self, col: Union[SortOrder, ColumnRef, str], session: "RemoteSparkSession"
    ) -> proto.Sort.SortField:
        if type(col) is SortOrder:
            so = cast(SortOrder, col)
            sf = proto.Sort.SortField()
            sf.expression.CopyFrom(so.ref.to_plan(session))
            sf.direction = (
                proto.Sort.SortDirection.SORT_ASCENDING
                if so.ascending
                else proto.Sort.SortDirection.SORT_DESCENDING
            )
            sf.nulls = (
                proto.Sort.SortNulls.NULLS_FIRST
                if not so.nullsLast
                else proto.Sort.SortNulls.NULLS_LAST
            )
            return sf
        else:
            sf = proto.Sort.SortField()
            # Check string
            if type(col) is ColumnRef:
                sf.expression.CopyFrom(cast(ColumnRef, col).to_plan(session))
            else:
                sf.expression.CopyFrom(self.unresolved_attr(cast(str, col)))
            sf.direction = proto.Sort.SortDirection.SORT_ASCENDING
            sf.nulls = proto.Sort.SortNulls.NULLS_LAST
            return sf

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        plan = proto.Relation()
        plan.sort.input.CopyFrom(self._child.plan(session))
        plan.sort.sort_fields.extend(
            [self.col_to_sort_field(x, session) for x in self.columns]
        )
        return plan

    def print(self, indent=0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{self._i(indent)}<Sort columns={self.columns}>\n{c_buf}"

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Sort</b><br />
                {", ".join([str(c) for c in self.columns])}
                {self._child._repr_html_()}
            </li>
        </uL>
        """


class Aggregate(LogicalPlan):
    MeasuresType = Sequence[Tuple[ExpressionOrString, str]]
    OptMeasuresType = Optional[MeasuresType]

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        grouping_cols: List[ColumnRef],
        measures: OptMeasuresType,
    ) -> None:
        super().__init__(child)
        self.grouping_cols = grouping_cols
        self.measures = measures if measures is not None else []

    def _convert_measure(self, m, session: "RemoteSparkSession"):
        exp, fun = m
        measure = proto.Aggregate.Measure()
        measure.function.name = fun
        if type(exp) is str:
            measure.function.arguments.append(self.unresolved_attr(exp))
        else:
            measure.function.arguments.append(cast(Expression, exp).to_plan(session))
        return measure

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        groupings = [x.to_plan(session) for x in self.grouping_cols]

        agg = proto.Relation()
        agg.aggregate.input.CopyFrom(self._child.plan(session))
        agg.aggregate.measures.extend(
            list(map(lambda x: self._convert_measure(x, session), self.measures))
        )

        gs = proto.Aggregate.GroupingSet()
        gs.aggregate_expressions.extend(groupings)
        agg.aggregate.grouping_sets.append(gs)
        return agg

    def print(self, indent=0) -> str:
        c_buf = self._child.print(indent + LogicalPlan.INDENT) if self._child else ""
        return f"{self._i(indent)}<Sort columns={self.grouping_cols} measures={self.measures}>\n{c_buf}"

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Aggregation</b><br />
                {self._child._repr_html_()}
            </li>
        </uL>
        """


class Join(LogicalPlan):
    def __init__(
        self,
        left: Optional["LogicalPlan"],
        right: "LogicalPlan",
        on: ColumnOrString,
        how: proto.Join.JoinType = proto.Join.JoinType.JOIN_INNER,
    ) -> None:
        super().__init__(left)
        self.left = cast(LogicalPlan, left)
        self.right = right
        self.on = on
        if how is None:
            how = proto.Join.JoinType.JOIN_INNER
        self.how = how

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        rel = proto.Relation()
        rel.join.left.CopyFrom(self.left.plan(session))
        rel.join.right.CopyFrom(self.right.plan(session))
        rel.join.on.CopyFrom(self.to_attr_or_expression(self.on, session))
        return rel

    def print(self, indent=0) -> str:
        i = self._i(indent)
        o = self._i(indent + LogicalPlan.INDENT)
        n = indent + LogicalPlan.INDENT * 2
        return f"""{i}<Join on={self.on} how={self.how}>\n{o}left=\n{self.left.print(n)}\n{o}right=\n{self.right.print(n)}"""

    def _repr_html_(self):
        return f"""
        <ul>
            <li>
                <b>Join</b><br />
                Left: {self.left._repr_html_()}
                Right: {self.right._repr_html_()}
            </li>
        </uL>
        """


class UnionAll(LogicalPlan):
    def __init__(self, child: Optional["LogicalPlan"], other: "LogicalPlan") -> None:
        super().__init__(child)
        self.other = other

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        assert self._child is not None
        rel = proto.Relation()
        rel.union.inputs.extend([self._child.plan(session), self.other.plan(session)])
        rel.union.union_type = proto.Union.UnionType.UNION_ALL

    def print(self, indent=0) -> str:
        assert self._child is not None
        assert self.other is not None

        i = self._i(indent)
        o = self._i(indent + LogicalPlan.INDENT)
        n = indent + LogicalPlan.INDENT * 2
        return f"""{i}UnionAll\n{o}child1=\n{self._child.print(n)}\n{o}child2=\n{self.other.print(n)}"""

    def _repr_html_(self) -> str:
        assert self._child is not None
        assert self.other is not None

        return f"""
        <ul>
            <li>
                <b>Union</b><br />
                Left: {self._child._repr_html_()}
                Right: {self.other._repr_html_()}
            </li>
        </uL>
        """


class Sql(LogicalPlan):
    def __init__(self, query: str) -> None:
        super().__init__(None)
        self._query = query

    def plan(self, session: "RemoteSparkSession") -> proto.Relation:
        rel = proto.Relation()
        rel.sql.query = self._query
        return rel


    def _repr_html_(self) -> str:
        return f"""
        <ul>
           <li>
              <b>SQL</b><br />
              Statement: <pre>{self._query}</pre>
           </li>
        </ul>
        """
