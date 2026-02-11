import React, {useEffect, useLayoutEffect, useRef, useState} from 'react'
import {Spinner} from 'react-bootstrap'
import UplotReact from 'uplot-react'
import uPlot, {AlignedData} from 'uplot'
import {ObjectiveType} from '../../App'
import {IconExternal} from '../Icons'
import {blues, greens, reds, yellows} from './colors'
import {seriesGaps} from './gaps'
import {PromiseClient} from '@connectrpc/connect'
import {ObjectiveService} from '../../proto/objectives/v1alpha1/objectives_connect'
import {Timestamp} from '@bufbuild/protobuf'
import {Series} from '../../proto/objectives/v1alpha1/objectives_pb'
import {selectTimeRange} from './selectTimeRange'
import {Labels, labelValues, labelsString} from '../../labels'
import {buildExternalHRef, externalName} from '../../external'

interface RequestsGraphProps {
  client: PromiseClient<typeof ObjectiveService>
  labels: Labels
  grouping: Labels
  from: number
  to: number
  uPlotCursor: uPlot.Cursor
  type: ObjectiveType
  updateTimeRange: (min: number, max: number, absolute: boolean) => void
  absolute: boolean
}

const RequestsGraph = ({
  client,
  labels: objectiveLabels,
  grouping,
  from,
  to,
  uPlotCursor,
  type,
  updateTimeRange,
  absolute = false,
}: RequestsGraphProps): JSX.Element => {
  const targetRef = useRef() as React.MutableRefObject<HTMLDivElement>

  const [width, setWidth] = useState<number>(500)
  const [data, setData] = useState<AlignedData>([])
  const [seriesLabels, setSeriesLabels] = useState<Labels[]>([])
  const [loading, setLoading] = useState<boolean>(true)
  const [error, setError] = useState<boolean>(false)
  const [queryStr, setQueryStr] = useState<string>('')

  const setWidthFromContainer = () => {
    if (targetRef?.current !== undefined && targetRef?.current !== null) {
      setWidth(targetRef.current.offsetWidth)
    }
  }

  // Set width on first render
  useLayoutEffect(setWidthFromContainer)
  // Set width on every window resize
  window.addEventListener('resize', setWidthFromContainer)

  useEffect(() => {
    setLoading(true)
    setError(false)
    client
      .graphRate({
        expr: labelsString(objectiveLabels),
        grouping: labelsString(grouping),
        start: Timestamp.fromDate(new Date(from)),
        end: Timestamp.fromDate(new Date(to)),
      })
      .then((resp) => {
        const timeseries = resp.timeseries
        if (timeseries !== undefined && timeseries.series.length >= 2) {
          const [x, ...series] = timeseries.series
          const timestamps = x.values
          const values = series.map((s: Series) => s.values)
          setData([timestamps, ...values])

          // Parse label strings into Labels objects
          const lbls: Labels[] = timeseries.labels.map((l: string) => {
            // Label strings are like {key="value",...} - parse them
            const parsed: Labels = {}
            const stripped = l.replace(/^\{|}$/g, '')
            if (stripped !== '') {
              stripped.split(',').forEach((pair) => {
                const eqIdx = pair.indexOf('=')
                if (eqIdx > 0) {
                  const key = pair.substring(0, eqIdx).trim()
                  let value = pair.substring(eqIdx + 1).trim()
                  value = value.replace(/^"|"$/g, '')
                  parsed[key] = value
                }
              })
            }
            return parsed
          })
          setSeriesLabels(lbls)
          setQueryStr(timeseries.query)
        } else {
          setData([])
          setSeriesLabels([])
        }
      })
      .catch(() => {
        setData([])
        setSeriesLabels([])
        setError(true)
      })
      .finally(() => {
        setLoading(false)
      })
  }, [client, objectiveLabels, grouping, from, to])

  if (loading) {
    return (
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        <h4 className="graphs-headline">
          {type === ObjectiveType.Ratio ? 'Requests' : 'Probes'}
          <Spinner
            animation="border"
            style={{
              marginLeft: '1rem',
              marginBottom: '0.5rem',
              width: '1rem',
              height: '1rem',
              borderWidth: '1px',
            }}
          />
        </h4>
      </div>
    )
  }

  if (error) {
    return (
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        error
      </div>
    )
  }

  // small state used while picking colors to reuse as little as possible
  const pickedColors = {
    greens: 0,
    yellows: 0,
    blues: 0,
    reds: 0,
  }

  let headline = 'Requests'
  let description = 'How many requests per second have there been?'
  if (type === ObjectiveType.BoolGauge) {
    headline = 'Probes'
    description = 'How many probes per second have there been?'
  }

  return (
    <div>
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        <h4 className="graphs-headline">{headline}</h4>
        {queryStr !== '' ? (
          <a
            className="external-prometheus"
            target="_blank"
            rel="noreferrer"
            href={buildExternalHRef([queryStr], from, to)}>
            <IconExternal height={20} width={20} />
            {externalName()}
          </a>
        ) : (
          <></>
        )}
      </div>
      <div>
        <p>{description}</p>
      </div>

      <div ref={targetRef}>
        {data.length > 0 ? (
          <UplotReact
            options={{
              width: width,
              height: 150,
              padding: [15, 0, 0, 0],
              cursor: uPlotCursor,
              series: [
                {},
                ...seriesLabels.map((label: Labels): uPlot.Series => {
                  const value = labelValues(label)[0]
                  return {
                    label: value,
                    stroke: `#${labelColor(pickedColors, value)}`,
                    gaps: seriesGaps(from / 1000, to / 1000),
                    value: (u, v) => (v == null ? '-' : `${v.toFixed(2)}req/s`),
                  }
                }),
              ],
              scales: {
                x: {min: from / 1000, max: to / 1000},
                y: {
                  range: {
                    min: absolute ? {hard: 0, mode: 1, soft: 0} : {hard: 0},
                    max: {},
                  },
                },
              },
              hooks: {
                setSelect: [selectTimeRange(updateTimeRange)],
              },
            }}
            data={data}
          />
        ) : (
          <UplotReact
            options={{
              width: width,
              height: 150,
              padding: [15, 0, 0, 0],
              series: [{}, {}],
              scales: {
                x: {min: from / 1000, max: to / 1000},
                y: {min: 0, max: 1},
              },
            }}
            data={[[], []]}
          />
        )}
      </div>
    </div>
  )
}

const labelColor = (picked: {[color: string]: number}, label: string): string => {
  label = label !== undefined ? label.toLowerCase() : ''
  let color = ''
  if (label === '{}' || label === '' || label === 'value') {
    color = greens[picked.greens % greens.length]
    picked.greens++
  }
  if (label.match(/(2\d{2}|2\w{2}|ok|noerror|hit)/) != null) {
    color = greens[picked.greens % greens.length]
    picked.greens++
  }
  if (label.match(/(3\d{2}|3\w{2})/) != null) {
    color = yellows[picked.yellows % yellows.length]
    picked.yellows++
  }
  if (
    label.match(
      /(4\d{2}|4\w{2}|canceled|invalidargument|notfound|alreadyexists|permissiondenied|unauthenticated|resourceexhausted|failedprecondition|aborted|outofrange|nxdomain|refused)/,
    ) != null
  ) {
    color = blues[picked.blues % blues.length]
    picked.blues++
  }
  if (
    label.match(
      /(5\d{2}|5\w{2}|unknown|deadlineexceeded|unimplemented|internal|unavailable|dataloss|servfail|miss)/,
    ) != null
  ) {
    color = reds[picked.reds % reds.length]
    picked.reds++
  }
  return color
}

export default RequestsGraph
