import {PromiseClient} from '@connectrpc/connect'
import {ObjectiveService} from '../../proto/objectives/v1alpha1/objectives_connect'
import uPlot, {AlignedData} from 'uplot'
import React, {useEffect, useLayoutEffect, useRef, useState} from 'react'
import UplotReact from 'uplot-react'
import {Spinner} from 'react-bootstrap'
import {seriesGaps} from './gaps'
import {blues, greys, reds} from './colors'
import {Alert, Timeseries} from '../../proto/objectives/v1alpha1/objectives_pb'
import {formatDuration} from '../../duration'
import {Timestamp} from '@bufbuild/protobuf'
import {Labels, labelsString} from '../../labels'

interface BurnrateGraphProps {
  client: PromiseClient<typeof ObjectiveService>
  alert: Alert
  labels: Labels
  grouping: Labels
  alertIndex: number
  threshold: number
  from: number
  to: number
  pendingData: AlignedData
  firingData: AlignedData
  uPlotCursor: uPlot.Cursor
}

// Convert a Timeseries proto (from ObjectiveService) to [timestamps, values] AlignedData
const timeseriesToAligned = (ts: Timeseries | undefined): AlignedData | null => {
  if (ts === undefined || ts.series.length < 2) {
    return null
  }
  const timestamps = ts.series[0].values
  const values = ts.series[1].values
  return [timestamps, values]
}

// Merge two AlignedData arrays (short and long) onto the same timestamp axis
const mergeBurnrateData = (
  shortData: AlignedData | null,
  longData: AlignedData | null,
): AlignedData => {
  if (shortData === null && longData === null) {
    return [[], [], []]
  }

  // Collect all unique timestamps from both
  const tsSet = new Map<number, {short: number | null; long: number | null}>()

  if (shortData !== null) {
    for (let i = 0; i < shortData[0].length; i++) {
      const t = shortData[0][i]
      const entry = tsSet.get(t) ?? {short: null, long: null}
      entry.short = shortData[1][i] ?? null
      tsSet.set(t, entry)
    }
  }

  if (longData !== null) {
    for (let i = 0; i < longData[0].length; i++) {
      const t = longData[0][i]
      const entry = tsSet.get(t) ?? {short: null, long: null}
      entry.long = longData[1][i] ?? null
      tsSet.set(t, entry)
    }
  }

  const sortedTimes = Array.from(tsSet.keys()).sort((a, b) => a - b)
  const shortValues: Array<number | null> = []
  const longValues: Array<number | null> = []

  for (const t of sortedTimes) {
    const entry = tsSet.get(t)
    shortValues.push(entry?.short ?? null)
    longValues.push(entry?.long ?? null)
  }

  return [sortedTimes, shortValues, longValues]
}

const BurnrateGraph = ({
  client,
  alert,
  labels,
  grouping,
  alertIndex,
  threshold,
  from,
  to,
  pendingData,
  firingData,
  uPlotCursor,
}: BurnrateGraphProps): JSX.Element => {
  const targetRef = useRef() as React.MutableRefObject<HTMLDivElement>

  const [width, setWidth] = useState<number>(500)
  const [loading, setLoading] = useState<boolean>(true)
  const [burnrateData, setBurnrateData] = useState<AlignedData>([[], [], []])

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
    client
      .graphBurnrate({
        expr: labelsString(labels),
        grouping: labelsString(grouping),
        start: Timestamp.fromDate(new Date(from)),
        end: Timestamp.fromDate(new Date(to)),
        alertIndex: alertIndex,
      })
      .then((resp) => {
        const shortAligned = timeseriesToAligned(resp.short)
        const longAligned = timeseriesToAligned(resp.long)
        setBurnrateData(mergeBurnrateData(shortAligned, longAligned))
      })
      .catch(() => {
        setBurnrateData([[], [], []])
      })
      .finally(() => {
        setLoading(false)
      })
  }, [client, labels, grouping, alertIndex, from, to])

  if (loading) {
    return (
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        <h4 className="graphs-headline">
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

  const timestamps = burnrateData[0] as number[]
  const shortSeries = burnrateData[1] as Array<number | null>
  const longSeries = burnrateData[2] as Array<number | null>

  const data: AlignedData = [
    timestamps,
    shortSeries,
    longSeries,
    // Add a sample for every timestamp with the threshold as value.
    Array(timestamps.length).fill(threshold),
  ]

  // no data
  if (timestamps.length === 0) {
    return (
      <div ref={targetRef} className="burnrate">
        <h5 className="graphs-headline">Burnrate</h5>
        <UplotReact
          options={{
            width: width - (2 * 10 + 2 * 15), // margin and padding
            height: 150,
            padding: [15, 0, 0, 0],
            cursor: uPlotCursor,
            series: [
              {},
              {
                min: 0,
                label: 'short',
                gaps: seriesGaps(from / 1000, to / 1000),
                stroke: `#${reds[1]}`,
                value: (u, v) => (v == null ? '-' : v.toFixed(2)),
              },
              {
                min: 0,
                label: 'long',
                gaps: seriesGaps(from / 1000, to / 1000),
                stroke: `#${reds[2]}`,
                value: (u, v) => (v == null ? '-' : v.toFixed(2)),
              },
              {
                label: 'threshold',
                stroke: `#${blues[0]}`,
              },
            ],
            scales: {
              x: {min: from / 1000, max: to / 1000},
            },
          }}
          data={[[], [], [], []]}
        />
      </div>
    )
  }

  const shortFormatted = formatDuration(Number(alert.short?.window?.seconds) * 1000 ?? 0)
  const longFormatted = formatDuration(Number(alert.long?.window?.seconds) * 1000 ?? 0)
  const pendingColor = 'rgb(244,163,42)'
  const pendingBackgroundColor = 'rgba(244,163,42,0.1)'
  const firingColor = 'rgb(244,99,99)'
  const firingBackgroundColor = 'rgba(244,99,99,0.1)'

  // Determine pending/firing series from props
  let pendingSeries: number[] | undefined
  if (pendingData.length > 0 && pendingData[0].length > 0) {
    // merge pending onto timestamps  
    const pendingMap = new Map<number, number>()
    for (let i = 0; i < pendingData[0].length; i++) {
      pendingMap.set(pendingData[0][i], pendingData[1]?.[i] ?? 0)
    }
    pendingSeries = timestamps.map((t) => pendingMap.get(t) ?? (null as unknown as number))
  }

  let firingSeries: number[] | undefined
  if (firingData.length > 0 && firingData[0].length > 0) {
    const firingMap = new Map<number, number>()
    for (let i = 0; i < firingData[0].length; i++) {
      firingMap.set(firingData[0][i], firingData[1]?.[i] ?? 0)
    }
    firingSeries = timestamps.map((t) => firingMap.get(t) ?? (null as unknown as number))
  }

  return (
    <div ref={targetRef} className="burnrate">
      <h5 className="graphs-headline">Burnrate</h5>
      <div className="graphs-description">
        <p>
          The short ({shortFormatted}) and long ({longFormatted}) burn rates <strong>both</strong>{' '}
          have to be over the {threshold.toFixed(2)}% threshold. <br />
          First, the alert is <i style={{color: pendingColor}}>pending</i> for{' '}
          {formatDuration(Number(alert.for?.seconds) * 1000)} and then the alert will be{' '}
          <i style={{color: firingColor}}>firing</i>.
        </p>
      </div>
      <UplotReact
        options={{
          width: width - (2 * 10 + 2 * 15), // margin and padding
          height: 150,
          padding: [15, 0, 0, 0],
          cursor: uPlotCursor,
          series: [
            {},
            {
              min: 0,
              label: `short (${shortFormatted})`,
              gaps: seriesGaps(from / 1000, to / 1000),
              stroke: '#42a5f5',
              value: (u, v) => (v == null ? '-' : v.toFixed(2)),
            },
            {
              min: 0,
              label: `long (${longFormatted})`,
              gaps: seriesGaps(from / 1000, to / 1000),
              stroke: '#651fff',
              value: (u, v) => (v == null ? '-' : v.toFixed(2)),
            },
            {
              label: 'threshold',
              stroke: `#${greys[0]}`,
              dash: [25, 10],
              value: (u, v) => (v == null ? '-' : v.toFixed(2)),
            },
          ],
          scales: {
            x: {min: from / 1000, max: to / 1000},
          },
          axes: [
            {},
            {
              values: (uplot: uPlot, v: number[]) => v.map((v: number) => `${v.toFixed(1)}`),
            },
          ],
          hooks: {
            drawAxes: [
              (u: uPlot) => {
                if (pendingSeries === undefined && firingSeries === undefined) {
                  return
                }

                const {ctx} = u
                const {top, height} = u.bbox
                ctx.save()

                let startPending: number = 0
                let startFiring: number = 0
                let drawingPending: boolean = false
                let drawingFiring: boolean = false

                for (let i = 0; i < timestamps.length; i++) {
                  const t = timestamps[i]
                  const cx = Math.round(u.valToPos(t, 'x', true))

                  if (firingSeries !== undefined) {
                    if (!drawingFiring && firingSeries[i] !== null) {
                      startFiring = cx
                      drawingFiring = true
                    }
                    if (drawingFiring && firingSeries[i] === null) {
                      ctx.fillStyle = firingBackgroundColor
                      ctx.fillRect(startFiring, top, cx - startFiring, height)
                      drawingFiring = false
                    }
                  }

                  if (pendingSeries !== undefined) {
                    if (!drawingPending && pendingSeries[i] !== null) {
                      startPending = cx
                      drawingPending = true
                    }
                    if (drawingPending && pendingSeries[i] === null) {
                      ctx.fillStyle = pendingBackgroundColor
                      ctx.fillRect(startPending, top, cx - startPending, height)
                      drawingPending = false
                    }
                  }
                }

                // position of last timestamp
                const cx = Math.round(u.valToPos(timestamps[timestamps.length - 1], 'x', true))

                // Firing until the very last timestamp, we need to draw the final rect
                if (drawingFiring) {
                  ctx.fillStyle = firingBackgroundColor
                  ctx.fillRect(startFiring, top, cx - startFiring, height)
                }

                // Pending until the very last timestamp, we need to draw the final rect
                if (drawingPending) {
                  ctx.fillStyle = pendingBackgroundColor
                  ctx.fillRect(startPending, top, cx - startFiring, height)
                }

                ctx.restore()
              },
            ],
          },
        }}
        data={data}
      />
    </div>
  )
}

export default BurnrateGraph
