/*-------------------------------------------------------------------------
 *
 * geo_ops_s.c
 *	  2D geometric operations static functions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/geo_ops_s.c
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/*
 * Internal routines
 */

enum path_delim
{
	PATH_NONE, PATH_OPEN, PATH_CLOSED
};

/*
 * Delimiters for input and output strings.
 * LDELIM, RDELIM, and DELIM are left, right, and separator delimiters, respectively.
 * LDELIM_EP, RDELIM_EP are left and right delimiters for paths with endpoints.
 */

#define LDELIM			'('
#define RDELIM			')'
#define DELIM			','
#define LDELIM_EP		'['
#define RDELIM_EP		']'
#define LDELIM_C		'<'
#define RDELIM_C		'>'

static int	point_inside(Point *p, int npts, Point *plist);
static int	lseg_crossing(double x, double y, double px, double py);
static BOX *box_construct(double x1, double x2, double y1, double y2);
static BOX *box_copy(BOX *box);
static BOX *box_fill(BOX *result, double x1, double x2, double y1, double y2);
static bool box_ov(BOX *box1, BOX *box2);
static double box_ht(BOX *box);
static double box_wd(BOX *box);
static double circle_ar(CIRCLE *circle);
static CIRCLE *circle_copy(CIRCLE *circle);
static LINE *line_construct_pm(Point *pt, double m);
static void line_construct_pts(LINE *line, Point *pt1, Point *pt2);
static bool lseg_intersect_internal(LSEG *l1, LSEG *l2);
static double lseg_dt(LSEG *l1, LSEG *l2);
static bool on_ps_internal(Point *pt, LSEG *lseg);
static void make_bound_box(POLYGON *poly);
static bool plist_same(int npts, Point *p1, Point *p2);
static Point *point_construct(double x, double y);
static Point *point_copy(Point *pt);
static void statlseg_construct(LSEG *lseg, Point *pt1, Point *pt2);
static double box_ar(BOX *box);
static void box_cn(Point *center, BOX *box);
static Point *interpt_sl(LSEG *lseg, LINE *line);
static bool has_interpt_sl(LSEG *lseg, LINE *line);
static double dist_pl_internal(Point *pt, LINE *line);
static double dist_ps_internal(Point *pt, LSEG *lseg);
static Point *line_interpt_internal(LINE *l1, LINE *l2);
static bool lseg_inside_poly(Point *a, Point *b, POLYGON *poly, int start);
static Point *lseg_interpt_internal(LSEG *l1, LSEG *l2);
static double dist_ppoly_internal(Point *pt, POLYGON *poly);

#define POINT_ON_POLYGON INT_MAX

static int
point_inside(Point *p, int npts, Point *plist)
{
	double		x0,
				y0;
	double		prev_x,
				prev_y;
	int			i = 0;
	double		x,
				y;
	int			cross,
				total_cross = 0;

	if (npts <= 0)
		return 0;

	/* compute first polygon point relative to single point */
	x0 = plist[0].x - p->x;
	y0 = plist[0].y - p->y;

	prev_x = x0;
	prev_y = y0;
	/* loop over polygon points and aggregate total_cross */
	for (i = 1; i < npts; i++)
	{
		/* compute next polygon point relative to single point */
		x = plist[i].x - p->x;
		y = plist[i].y - p->y;

		/* compute previous to current point crossing */
		if ((cross = lseg_crossing(x, y, prev_x, prev_y)) == POINT_ON_POLYGON)
			return 2;
		total_cross += cross;

		prev_x = x;
		prev_y = y;
	}

	/* now do the first point */
	if ((cross = lseg_crossing(x0, y0, prev_x, prev_y)) == POINT_ON_POLYGON)
		return 2;
	total_cross += cross;

	if (total_cross != 0)
		return 1;
	return 0;
}

/* lseg_crossing()
 * Returns +/-2 if line segment crosses the positive X-axis in a +/- direction.
 * Returns +/-1 if one point is on the positive X-axis.
 * Returns 0 if both points are on the positive X-axis, or there is no crossing.
 * Returns POINT_ON_POLYGON if the segment contains (0,0).
 * Wow, that is one confusing API, but it is used above, and when summed,
 * can tell is if a point is in a polygon.
 */

static int
lseg_crossing(double x, double y, double prev_x, double prev_y)
{
	double		z;
	int			y_sign;

	if (FPzero(y))
	{							/* y == 0, on X axis */
		if (FPzero(x))			/* (x,y) is (0,0)? */
			return POINT_ON_POLYGON;
		else if (FPgt(x, 0))
		{						/* x > 0 */
			if (FPzero(prev_y)) /* y and prev_y are zero */
				/* prev_x > 0? */
				return FPgt(prev_x, 0) ? 0 : POINT_ON_POLYGON;
			return FPlt(prev_y, 0) ? 1 : -1;
		}
		else
		{						/* x < 0, x not on positive X axis */
			if (FPzero(prev_y))
				/* prev_x < 0? */
				return FPlt(prev_x, 0) ? 0 : POINT_ON_POLYGON;
			return 0;
		}
	}
	else
	{							/* y != 0 */
		/* compute y crossing direction from previous point */
		y_sign = FPgt(y, 0) ? 1 : -1;

		if (FPzero(prev_y))
			/* previous point was on X axis, so new point is either off or on */
			return FPlt(prev_x, 0) ? 0 : y_sign;
		else if (FPgt(y_sign * prev_y, 0))
			/* both above or below X axis */
			return 0;			/* same sign */
		else
		{						/* y and prev_y cross X-axis */
			if (FPge(x, 0) && FPgt(prev_x, 0))
				/* both non-negative so cross positive X-axis */
				return 2 * y_sign;
			if (FPlt(x, 0) && FPle(prev_x, 0))
				/* both non-positive so do not cross positive X-axis */
				return 0;

			/* x and y cross axises, see URL above point_inside() */
			z = (x - prev_x) * y - (y - prev_y) * x;
			if (FPzero(z))
				return POINT_ON_POLYGON;
			return FPgt((y_sign * z), 0) ? 0 : 2 * y_sign;
		}
	}
}


/*		box_construct	-		fill in a new box.
 */
static BOX *
box_construct(double x1, double x2, double y1, double y2)
{
	BOX		   *result = (BOX *) palloc(sizeof(BOX));

	return box_fill(result, x1, x2, y1, y2);
}

/*		box_copy		-		copy a box
 */
static BOX *
box_copy(BOX *box)
{
	BOX		   *result = (BOX *) palloc(sizeof(BOX));

	memcpy((char *) result, (char *) box, sizeof(BOX));

	return result;
}

/*		box_fill		-		fill in a given box struct
 */
static BOX *
box_fill(BOX *result, double x1, double x2, double y1, double y2)
{
	if (x1 > x2)
	{
		result->high.x = x1;
		result->low.x = x2;
	}
	else
	{
		result->high.x = x2;
		result->low.x = x1;
	}
	if (y1 > y2)
	{
		result->high.y = y1;
		result->low.y = y2;
	}
	else
	{
		result->high.y = y2;
		result->low.y = y1;
	}

	return result;
}

static bool
box_ov(BOX *box1, BOX *box2)
{
	return (FPle(box1->low.x, box2->high.x) &&
			FPle(box2->low.x, box1->high.x) &&
			FPle(box1->low.y, box2->high.y) &&
			FPle(box2->low.y, box1->high.y));
}

/*		box_ht	-		returns the height of the box
 *								  (vertical magnitude).
 */
static double
box_ht(BOX *box)
{
	return box->high.y - box->low.y;
}

/*		box_wd	-		returns the width (length) of the box
 *								  (horizontal magnitude).
 */
static double
box_wd(BOX *box)
{
	return box->high.x - box->low.x;
}


/*		circle_ar		-		returns the area of the circle.
 */
static double
circle_ar(CIRCLE *circle)
{
	return M_PI * (circle->radius * circle->radius);
}

/*----------------------------------------------------------
 *	"Arithmetic" operators on circles.
 *---------------------------------------------------------*/

static CIRCLE *
circle_copy(CIRCLE *circle)
{
	CIRCLE	   *result;

	if (!PointerIsValid(circle))
		return NULL;

	result = (CIRCLE *) palloc(sizeof(CIRCLE));
	memcpy((char *) result, (char *) circle, sizeof(CIRCLE));
	return result;
}

/*----------------------------------------------------------
 *	Conversion routines from one line formula to internal.
 *		Internal form:	Ax+By+C=0
 *---------------------------------------------------------*/

/* line_construct_pm()
 * point-slope
 */
static LINE *
line_construct_pm(Point *pt, double m)
{
	LINE	   *result = (LINE *) palloc(sizeof(LINE));

	if (m == DBL_MAX)
	{
		/* vertical - use "x = C" */
		result->A = -1;
		result->B = 0;
		result->C = pt->x;
	}
	else
	{
		/* use "mx - y + yinter = 0" */
		result->A = m;
		result->B = -1.0;
		result->C = pt->y - m * pt->x;
	}

	return result;
}

/*
 * Fill already-allocated LINE struct from two points on the line
 */
static void
line_construct_pts(LINE *line, Point *pt1, Point *pt2)
{
	if (FPeq(pt1->x, pt2->x))
	{							/* vertical */
		/* use "x = C" */
		line->A = -1;
		line->B = 0;
		line->C = pt1->x;
#ifdef GEODEBUG
		printf("line_construct_pts- line is vertical\n");
#endif
	}
	else if (FPeq(pt1->y, pt2->y))
	{							/* horizontal */
		/* use "y = C" */
		line->A = 0;
		line->B = -1;
		line->C = pt1->y;
#ifdef GEODEBUG
		printf("line_construct_pts- line is horizontal\n");
#endif
	}
	else
	{
		/* use "mx - y + yinter = 0" */
		line->A = (pt2->y - pt1->y) / (pt2->x - pt1->x);
		line->B = -1.0;
		line->C = pt1->y - line->A * pt1->x;
		/* on some platforms, the preceding expression tends to produce -0 */
		if (line->C == 0.0)
			line->C = 0.0;
#ifdef GEODEBUG
		printf("line_construct_pts- line is neither vertical nor horizontal (diffs x=%.*g, y=%.*g\n",
			   DBL_DIG, (pt2->x - pt1->x), DBL_DIG, (pt2->y - pt1->y));
#endif
	}
}

static bool
lseg_intersect_internal(LSEG *l1, LSEG *l2)
{
	LINE		ln;
	Point	   *interpt;
	bool		retval;

	line_construct_pts(&ln, &l2->p[0], &l2->p[1]);
	interpt = interpt_sl(l1, &ln);

	if (interpt != NULL && on_ps_internal(interpt, l2))
		retval = true;			/* interpt on l1 and l2 */
	else
		retval = false;
	return retval;
}

static double
dist_ps_internal(Point *pt, LSEG *lseg)
{
	double		m;				/* slope of perp. */
	LINE	   *ln;
	double		result,
				tmpdist;
	Point	   *ip;

	/*
	 * Construct a line perpendicular to the input segment and through the
	 * input point
	 */
	if (lseg->p[1].x == lseg->p[0].x)
		m = 0;
	else if (lseg->p[1].y == lseg->p[0].y)
		m = (double) DBL_MAX;	/* slope is infinite */
	else
		m = (lseg->p[0].x - lseg->p[1].x) / (lseg->p[1].y - lseg->p[0].y);
	ln = line_construct_pm(pt, m);

#ifdef GEODEBUG
	printf("dist_ps- line is A=%g B=%g C=%g from (point) slope (%f,%f) %g\n",
		   ln->A, ln->B, ln->C, pt->x, pt->y, m);
#endif

	/*
	 * Calculate distance to the line segment or to the nearest endpoint of
	 * the segment.
	 */

	/* intersection is on the line segment? */
	if ((ip = interpt_sl(lseg, ln)) != NULL)
	{
		/* yes, so use distance to the intersection point */
		result = point_dt(pt, ip);
#ifdef GEODEBUG
		printf("dist_ps- distance is %f to intersection point is (%f,%f)\n",
			   result, ip->x, ip->y);
#endif
	}
	else
	{
		/* no, so use distance to the nearer endpoint */
		result = point_dt(pt, &lseg->p[0]);
		tmpdist = point_dt(pt, &lseg->p[1]);
		if (tmpdist < result)
			result = tmpdist;
	}

	return result;
}

/* lseg_dt()
 * Distance between two line segments.
 * Must check both sets of endpoints to ensure minimum distance is found.
 * - thomas 1998-02-01
 */
static double
lseg_dt(LSEG *l1, LSEG *l2)
{
	double		result,
				d;

	if (lseg_intersect_internal(l1, l2))
		return 0.0;

	d = dist_ps_internal(&l1->p[0], l2);
	result = d;
	d = dist_ps_internal(&l1->p[1], l2);
	result = Min(result, d);
	d = dist_ps_internal(&l2->p[0], l1);
	result = Min(result, d);
	d = dist_ps_internal(&l2->p[1], l1);
	result = Min(result, d);

	return result;
}

static bool
on_ps_internal(Point *pt, LSEG *lseg)
{
	return FPeq(point_dt(pt, &lseg->p[0]) + point_dt(pt, &lseg->p[1]),
				point_dt(&lseg->p[0], &lseg->p[1]));
}


/*------------------------------------------------------------------
 * The following routines define a data type and operator class for
 * POLYGONS .... Part of which (the polygon's bounding box) is built on
 * top of the BOX data type.
 *
 * make_bound_box - create the bounding box for the input polygon
 *------------------------------------------------------------------*/

/*---------------------------------------------------------------------
 * Make the smallest bounding box for the given polygon.
 *---------------------------------------------------------------------*/
static void
make_bound_box(POLYGON *poly)
{
	int			i;
	double		x1,
				y1,
				x2,
				y2;

	if (poly->npts > 0)
	{
		x2 = x1 = poly->p[0].x;
		y2 = y1 = poly->p[0].y;
		for (i = 1; i < poly->npts; i++)
		{
			if (poly->p[i].x < x1)
				x1 = poly->p[i].x;
			if (poly->p[i].x > x2)
				x2 = poly->p[i].x;
			if (poly->p[i].y < y1)
				y1 = poly->p[i].y;
			if (poly->p[i].y > y2)
				y2 = poly->p[i].y;
		}

		box_fill(&(poly->boundbox), x1, x2, y1, y2);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot create bounding box for empty polygon")));
}


static bool
plist_same(int npts, Point *p1, Point *p2)
{
	int			i,
				ii,
				j;

	/* find match for first point */
	for (i = 0; i < npts; i++)
	{
		if ((FPeq(p2[i].x, p1[0].x))
			&& (FPeq(p2[i].y, p1[0].y)))
		{

			/* match found? then look forward through remaining points */
			for (ii = 1, j = i + 1; ii < npts; ii++, j++)
			{
				if (j >= npts)
					j = 0;
				if ((!FPeq(p2[j].x, p1[ii].x))
					|| (!FPeq(p2[j].y, p1[ii].y)))
				{
#ifdef GEODEBUG
					printf("plist_same- %d failed forward match with %d\n", j, ii);
#endif
					break;
				}
			}
#ifdef GEODEBUG
			printf("plist_same- ii = %d/%d after forward match\n", ii, npts);
#endif
			if (ii == npts)
				return TRUE;

			/* match not found forwards? then look backwards */
			for (ii = 1, j = i - 1; ii < npts; ii++, j--)
			{
				if (j < 0)
					j = (npts - 1);
				if ((!FPeq(p2[j].x, p1[ii].x))
					|| (!FPeq(p2[j].y, p1[ii].y)))
				{
#ifdef GEODEBUG
					printf("plist_same- %d failed reverse match with %d\n", j, ii);
#endif
					break;
				}
			}
#ifdef GEODEBUG
			printf("plist_same- ii = %d/%d after reverse match\n", ii, npts);
#endif
			if (ii == npts)
				return TRUE;
		}
	}

	return FALSE;
}


static Point *
point_construct(double x, double y)
{
	Point	   *result = (Point *) palloc(sizeof(Point));

	result->x = x;
	result->y = y;
	return result;
}


static Point *
point_copy(Point *pt)
{
	Point	   *result;

	if (!PointerIsValid(pt))
		return NULL;

	result = (Point *) palloc(sizeof(Point));

	result->x = pt->x;
	result->y = pt->y;
	return result;
}

							/* path_encode() */

/* like lseg_construct, but assume space already allocated */
static void
statlseg_construct(LSEG *lseg, Point *pt1, Point *pt2)
{
	lseg->p[0].x = pt1->x;
	lseg->p[0].y = pt1->y;
	lseg->p[1].x = pt2->x;
	lseg->p[1].y = pt2->y;
}


/*		box_ar	-		returns the area of the box.
 */
static double
box_ar(BOX *box)
{
	return box_wd(box) * box_ht(box);
}


/*		box_cn	-		stores the centerpoint of the box into *center.
 */
static void
box_cn(Point *center, BOX *box)
{
	center->x = (box->high.x + box->low.x) / 2.0;
	center->y = (box->high.y + box->low.y) / 2.0;
}

/*---------------------------------------------------------------------
 *		interpt_
 *				Intersection point of objects.
 *				We choose to ignore the "point" of intersection between
 *				  lines and boxes, since there are typically two.
 *-------------------------------------------------------------------*/

/* Get intersection point of lseg and line; returns NULL if no intersection */
static Point *
interpt_sl(LSEG *lseg, LINE *line)
{
	LINE		tmp;
	Point	   *p;

	line_construct_pts(&tmp, &lseg->p[0], &lseg->p[1]);
	p = line_interpt_internal(&tmp, line);
#ifdef GEODEBUG
	printf("interpt_sl- segment is (%.*g %.*g) (%.*g %.*g)\n",
		   DBL_DIG, lseg->p[0].x, DBL_DIG, lseg->p[0].y, DBL_DIG, lseg->p[1].x, DBL_DIG, lseg->p[1].y);
	printf("interpt_sl- segment becomes line A=%.*g B=%.*g C=%.*g\n",
		   DBL_DIG, tmp.A, DBL_DIG, tmp.B, DBL_DIG, tmp.C);
#endif
	if (PointerIsValid(p))
	{
#ifdef GEODEBUG
		printf("interpt_sl- intersection point is (%.*g %.*g)\n", DBL_DIG, p->x, DBL_DIG, p->y);
#endif
		if (on_ps_internal(p, lseg))
		{
#ifdef GEODEBUG
			printf("interpt_sl- intersection point is on segment\n");
#endif
		}
		else
			p = NULL;
	}

	return p;
}

/* variant: just indicate if intersection point exists */
static bool
has_interpt_sl(LSEG *lseg, LINE *line)
{
	Point	   *tmp;

	tmp = interpt_sl(lseg, line);
	if (tmp)
		return true;
	return false;
}

static double
dist_pl_internal(Point *pt, LINE *line)
{
	return fabs((line->A * pt->x + line->B * pt->y + line->C) /
				HYPOT(line->A, line->B));
}

/*
 * Internal version of line_interpt
 *
 * returns a NULL pointer if no intersection point
 */
static Point *
line_interpt_internal(LINE *l1, LINE *l2)
{
	Point	   *result;
	double		x,
				y;

	/*
	 * NOTE: if the lines are identical then we will find they are parallel
	 * and report "no intersection".  This is a little weird, but since
	 * there's no *unique* intersection, maybe it's appropriate behavior.
	 */
	if (DatumGetBool(DirectFunctionCall2(line_parallel,
										 LinePGetDatum(l1),
										 LinePGetDatum(l2))))
		return NULL;

	if (FPzero(l1->B))			/* l1 vertical? */
	{
		x = l1->C;
		y = (l2->A * x + l2->C);
	}
	else if (FPzero(l2->B))		/* l2 vertical? */
	{
		x = l2->C;
		y = (l1->A * x + l1->C);
	}
	else
	{
		x = (l1->C - l2->C) / (l2->A - l1->A);
		y = (l1->A * x + l1->C);
	}
	result = point_construct(x, y);

#ifdef GEODEBUG
	printf("line_interpt- lines are A=%.*g, B=%.*g, C=%.*g, A=%.*g, B=%.*g, C=%.*g\n",
		   DBL_DIG, l1->A, DBL_DIG, l1->B, DBL_DIG, l1->C, DBL_DIG, l2->A, DBL_DIG, l2->B, DBL_DIG, l2->C);
	printf("line_interpt- lines intersect at (%.*g,%.*g)\n", DBL_DIG, x, DBL_DIG, y);
#endif

	return result;
}

/*
 * Tests special kind of segment for in/out of polygon.
 * Special kind means:
 *	- point a should be on segment s
 *	- segment (a,b) should not be contained by s
 * Returns true if:
 *	- segment (a,b) is collinear to s and (a,b) is in polygon
 *	- segment (a,b) s not collinear to s. Note: that doesn't
 *	  mean that segment is in polygon!
 */

static bool
touched_lseg_inside_poly(Point *a, Point *b, LSEG *s, POLYGON *poly, int start)
{
	/* point a is on s, b is not */
	LSEG		t;

	t.p[0] = *a;
	t.p[1] = *b;

#define POINTEQ(pt1, pt2)	(FPeq((pt1)->x, (pt2)->x) && FPeq((pt1)->y, (pt2)->y))
	if (POINTEQ(a, s->p))
	{
		if (on_ps_internal(s->p + 1, &t))
			return lseg_inside_poly(b, s->p + 1, poly, start);
	}
	else if (POINTEQ(a, s->p + 1))
	{
		if (on_ps_internal(s->p, &t))
			return lseg_inside_poly(b, s->p, poly, start);
	}
	else if (on_ps_internal(s->p, &t))
	{
		return lseg_inside_poly(b, s->p, poly, start);
	}
	else if (on_ps_internal(s->p + 1, &t))
	{
		return lseg_inside_poly(b, s->p + 1, poly, start);
	}

	return true;				/* may be not true, but that will check later */
}




/*
 * Returns true if segment (a,b) is in polygon, option
 * start is used for optimization - function checks
 * polygon's edges started from start
 */
static bool
lseg_inside_poly(Point *a, Point *b, POLYGON *poly, int start)
{
	LSEG		s,
				t;
	int			i;
	bool		res = true,
				intersection = false;

	t.p[0] = *a;
	t.p[1] = *b;
	s.p[0] = poly->p[(start == 0) ? (poly->npts - 1) : (start - 1)];

	for (i = start; i < poly->npts && res; i++)
	{
		Point	   *interpt;

		CHECK_FOR_INTERRUPTS();

		s.p[1] = poly->p[i];

		if (on_ps_internal(t.p, &s))
		{
			if (on_ps_internal(t.p + 1, &s))
				return true;	/* t is contained by s */

			/* Y-cross */
			res = touched_lseg_inside_poly(t.p, t.p + 1, &s, poly, i + 1);
		}
		else if (on_ps_internal(t.p + 1, &s))
		{
			/* Y-cross */
			res = touched_lseg_inside_poly(t.p + 1, t.p, &s, poly, i + 1);
		}
		else if ((interpt = lseg_interpt_internal(&t, &s)) != NULL)
		{
			/*
			 * segments are X-crossing, go to check each subsegment
			 */

			intersection = true;
			res = lseg_inside_poly(t.p, interpt, poly, i + 1);
			if (res)
				res = lseg_inside_poly(t.p + 1, interpt, poly, i + 1);
			pfree(interpt);
		}

		s.p[0] = s.p[1];
	}

	if (res && !intersection)
	{
		Point		p;

		/*
		 * if X-intersection wasn't found  then check central point of tested
		 * segment. In opposite case we already check all subsegments
		 */
		p.x = (t.p[0].x + t.p[1].x) / 2.0;
		p.y = (t.p[0].y + t.p[1].y) / 2.0;

		res = point_inside(&p, poly->npts, poly->p);
	}

	return res;
}

static Point *
lseg_interpt_internal(LSEG *l1, LSEG *l2)
{
	Point	   *result;
	LINE		tmp1,
				tmp2;

	/*
	 * Find the intersection of the appropriate lines, if any.
	 */
	line_construct_pts(&tmp1, &l1->p[0], &l1->p[1]);
	line_construct_pts(&tmp2, &l2->p[0], &l2->p[1]);
	result = line_interpt_internal(&tmp1, &tmp2);
	if (!PointerIsValid(result))
		return NULL;

	/*
	 * If the line intersection point isn't within l1 (or equivalently l2),
	 * there is no valid segment intersection point at all.
	 */
	if (!on_ps_internal(result, l1) ||
		!on_ps_internal(result, l2))
	{
		pfree(result);
		return NULL;
	}

	/*
	 * If there is an intersection, then check explicitly for matching
	 * endpoints since there may be rounding effects with annoying lsb
	 * residue. - tgl 1997-07-09
	 */
	if ((FPeq(l1->p[0].x, l2->p[0].x) && FPeq(l1->p[0].y, l2->p[0].y)) ||
		(FPeq(l1->p[0].x, l2->p[1].x) && FPeq(l1->p[0].y, l2->p[1].y)))
	{
		result->x = l1->p[0].x;
		result->y = l1->p[0].y;
	}
	else if ((FPeq(l1->p[1].x, l2->p[0].x) && FPeq(l1->p[1].y, l2->p[0].y)) ||
			 (FPeq(l1->p[1].x, l2->p[1].x) && FPeq(l1->p[1].y, l2->p[1].y)))
	{
		result->x = l1->p[1].x;
		result->y = l1->p[1].y;
	}

	return result;
}



static double
dist_ppoly_internal(Point *pt, POLYGON *poly)
{
	float8		result;
	float8		d;
	int			i;
	LSEG		seg;

	if (point_inside(pt, poly->npts, poly->p) != 0)
	{
#ifdef GEODEBUG
		printf("dist_ppoly_internal- point inside of polygon\n");
#endif
		return 0.0;
	}

	/* initialize distance with segment between first and last points */
	seg.p[0].x = poly->p[0].x;
	seg.p[0].y = poly->p[0].y;
	seg.p[1].x = poly->p[poly->npts - 1].x;
	seg.p[1].y = poly->p[poly->npts - 1].y;
	result = dist_ps_internal(pt, &seg);
#ifdef GEODEBUG
	printf("dist_ppoly_internal- segment 0/n distance is %f\n", result);
#endif

	/* check distances for other segments */
	for (i = 0; (i < poly->npts - 1); i++)
	{
		seg.p[0].x = poly->p[i].x;
		seg.p[0].y = poly->p[i].y;
		seg.p[1].x = poly->p[i + 1].x;
		seg.p[1].y = poly->p[i + 1].y;
		d = dist_ps_internal(pt, &seg);
#ifdef GEODEBUG
		printf("dist_ppoly_internal- segment %d distance is %f\n", (i + 1), d);
#endif
		if (d < result)
			result = d;
	}

	return result;
}




